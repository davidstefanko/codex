use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_final_assistant_message_sse_response;
use app_test_support::create_mock_responses_server_sequence;
use codex_app_server_protocol::*;
use codex_features::Feature;
use core_test_support::responses;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn thread_processes_trusted_client_selects_only_one_threads_live_processes() -> Result<()> {
    let responses = ["own-call", "own-call-2", "other-call", "other-call-2"]
        .into_iter()
        .flat_map(|call_id| {
            // Shell-specific syntax is chosen for the execution target, including remote executors.
            let cmd = if core_test_support::test_target_os()
                == core_test_support::TestTargetOs::Windows
            {
                "Start-Sleep -Seconds 60 # synthetic-argument-marker"
            } else {
                "sleep 60 # synthetic-argument-marker"
            };
            [
                responses::sse(vec![
                    responses::ev_response_created(call_id),
                    responses::ev_function_call(
                        call_id,
                        "exec_command",
                        &json!({
                            "cmd": cmd, "yield_time_ms": 1000, "login": false,
                        })
                        .to_string(),
                    ),
                    responses::ev_completed(call_id),
                ]),
                create_final_assistant_message_sse_response("done").expect("fixture SSE"),
            ]
        })
        .collect();
    let server = create_mock_responses_server_sequence(responses).await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(&server.uri())
        .with_sandbox_mode("danger-full-access")
        .enable_feature(Feature::UnifiedExec)
        .write(home.path())?;
    let mut client = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let mut threads = Vec::new();
    for call_id in ["own-call", "other-call"] {
        let ThreadStartResponse { thread, .. } =
            client.start_thread(ThreadStartParams::default()).await?;
        for call_id in [call_id.to_string(), format!("{call_id}-2")] {
            let _: TurnStartResponse = client
                .request(|request_id| ClientRequest::TurnStart {
                    request_id,
                    params: TurnStartParams {
                        thread_id: thread.id.clone(),
                        input: vec![UserInput::Text {
                            text: call_id.to_string(),
                            text_elements: Vec::new(),
                        }],
                        ..Default::default()
                    },
                })
                .await?;
            timeout(
                Duration::from_secs(/*secs*/ 20),
                client.read_stream_until_notification_message("turn/completed"),
            )
            .await??;
        }
        threads.push(thread.id);
    }
    // The app-server client owns a host-wide session, not a calling-thread identity.
    // Selecting another loaded thread is legitimate here; it must never union inventories.
    for (thread_id, item_id) in threads.iter().zip(["own-call", "other-call"]) {
        let result: ThreadProcessesListResponse = client
            .request(|request_id| ClientRequest::ThreadProcessesList {
                request_id,
                params: ThreadProcessesListParams {
                    thread_id: thread_id.clone(),
                    cursor: None,
                    limit: Some(1),
                },
            })
            .await?;
        assert_eq!(result.data.len(), 1);
        let process_id = result.data[0].process_id.clone();
        let first_item = result.data[0].item_id.clone();
        let second_item = if first_item == item_id {
            format!("{item_id}-2")
        } else {
            assert_eq!(first_item, format!("{item_id}-2"));
            item_id.to_string()
        };
        assert_eq!(
            serde_json::to_value(&result)?,
            json!({
                "data": [{ "processId": process_id, "itemId": first_item, "executable": "unified-exec", "status": "running" }],
                "nextCursor": process_id
            })
        );
        let after: ThreadProcessesListResponse = client
            .request(|request_id| ClientRequest::ThreadProcessesList {
                request_id,
                params: ThreadProcessesListParams {
                    thread_id: thread_id.clone(),
                    cursor: Some(process_id.clone()),
                    limit: None,
                },
            })
            .await?;
        assert_eq!(after.data.len(), 1);
        let second_id = after.data[0].process_id.clone();
        assert_eq!(
            after,
            ThreadProcessesListResponse {
                data: vec![ThreadTaskProcess {
                    process_id: second_id.clone(),
                    item_id: second_item,
                    executable: "unified-exec".to_string(),
                    status: ThreadTaskProcessStatus::Running
                }],
                next_cursor: None,
            }
        );
        let _: ThreadBackgroundTerminalsTerminateResponse = client
            .request(
                |request_id| ClientRequest::ThreadBackgroundTerminalsTerminate {
                    request_id,
                    params: ThreadBackgroundTerminalsTerminateParams {
                        thread_id: thread_id.clone(),
                        process_id,
                    },
                },
            )
            .await?;
        let exited: ThreadProcessesListResponse = client
            .request(|request_id| ClientRequest::ThreadProcessesList {
                request_id,
                params: ThreadProcessesListParams {
                    thread_id: thread_id.clone(),
                    cursor: None,
                    limit: None,
                },
            })
            .await?;
        assert_eq!(exited, after);
        let _: ThreadBackgroundTerminalsTerminateResponse = client
            .request(
                |request_id| ClientRequest::ThreadBackgroundTerminalsTerminate {
                    request_id,
                    params: ThreadBackgroundTerminalsTerminateParams {
                        thread_id: thread_id.clone(),
                        process_id: second_id,
                    },
                },
            )
            .await?;
        let empty: ThreadProcessesListResponse = client
            .request(|request_id| ClientRequest::ThreadProcessesList {
                request_id,
                params: ThreadProcessesListParams {
                    thread_id: thread_id.clone(),
                    cursor: None,
                    limit: Some(0),
                },
            })
            .await?;
        assert_eq!(
            empty,
            ThreadProcessesListResponse {
                data: Vec::new(),
                next_cursor: None
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn thread_processes_rejects_thread_from_another_runtime_and_invalid_cursor() -> Result<()> {
    let server = create_mock_responses_server_sequence(vec![]).await;
    let mut clients = Vec::new();
    let mut homes = Vec::new();
    for _ in 0..2 {
        let home = TempDir::new()?;
        MockResponsesConfig::new(&server.uri()).write(home.path())?;
        clients.push(
            TestAppServer::builder()
                .with_codex_home(home.path())
                .build_initialized()
                .await?,
        );
        homes.push(home);
    }
    let ThreadStartResponse { thread, .. } = clients[0]
        .start_thread(ThreadStartParams::default())
        .await?;
    let id = clients[1]
        .send_request(
            "thread/processes/list",
            Some(json!({ "threadId": thread.id })),
        )
        .await?;
    let error = timeout(
        Duration::from_secs(/*secs*/ 10),
        clients[1].read_stream_until_error_message(RequestId::Integer(id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert!(error.error.message.starts_with("thread not found:"));
    let id = clients[0]
        .send_request(
            "thread/processes/list",
            Some(json!({ "threadId": thread.id, "cursor": "invalid" })),
        )
        .await?;
    let error = timeout(
        Duration::from_secs(/*secs*/ 10),
        clients[0].read_stream_until_error_message(RequestId::Integer(id)),
    )
    .await??;
    assert_eq!(error.error.message, "invalid process cursor");
    Ok(())
}

#[tokio::test]
async fn thread_processes_requires_experimental_opt_in() -> Result<()> {
    let home = TempDir::new()?;
    let mut client = TestAppServer::builder()
        .with_codex_home(home.path())
        .build()
        .await?;
    client
        .initialize_with_capabilities(
            ClientInfo {
                name: "process-test".to_string(),
                title: None,
                version: "1".to_string(),
            },
            Some(InitializeCapabilities {
                experimental_api: false,
                ..Default::default()
            }),
        )
        .await?;
    let id = client
        .send_request(
            "thread/processes/list",
            Some(json!({ "threadId": "unloaded" })),
        )
        .await?;
    let error = timeout(
        Duration::from_secs(/*secs*/ 10),
        client.read_stream_until_error_message(RequestId::Integer(id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert!(error.error.message.contains("experimentalApi"));
    Ok(())
}
