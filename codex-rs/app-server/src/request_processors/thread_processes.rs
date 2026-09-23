//! Sanitized inventory for trusted app-server clients, not a task authorization API.

use super::*;

impl ThreadRequestProcessor {
    pub(crate) async fn thread_processes_list(
        &self,
        params: ThreadProcessesListParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let ThreadProcessesListParams {
            thread_id,
            cursor,
            limit,
        } = params;
        let after = cursor
            .map(|cursor| cursor.parse::<i32>())
            .transpose()
            .map_err(|_| invalid_request("invalid process cursor"))?;
        // Like thread/read, this RPC selects from the host's loaded threads.
        // A model-facing adapter must bind thread_id to its authenticated caller.
        // Neither a supplied threadId nor a notification subscription is authority.
        let (_, thread) = self.load_thread(&thread_id).await?;
        let mut data = thread
            .list_task_processes()
            .await
            .into_iter()
            .filter(|process| after.is_none_or(|after| process.process_id > after))
            .map(|process| ThreadTaskProcess {
                process_id: process.process_id.to_string(),
                item_id: process.item_id,
                executable: "unified-exec".to_string(),
                status: ThreadTaskProcessStatus::Running,
            })
            .collect::<Vec<_>>();
        let limit = limit.unwrap_or(/*default*/ 64).clamp(/*min*/ 1, /*max*/ 64) as usize;
        let next_cursor = (data.len() > limit).then(|| data[limit - 1].process_id.clone());
        data.truncate(limit);
        Ok(Some(
            ThreadProcessesListResponse { data, next_cursor }.into(),
        ))
    }
}
