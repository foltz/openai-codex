use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::ToolSearchOutput;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::tool_search_spec::create_tool_search_tool;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use bm25::Document;
use bm25::Language;
use bm25::SearchEngine;
use bm25::SearchEngineBuilder;
use codex_tools::LoadableToolSpec;
use codex_tools::TOOL_SEARCH_DEFAULT_LIMIT;
use codex_tools::TOOL_SEARCH_TOOL_NAME;
use codex_tools::ToolName;
use codex_tools::ToolSearchEntry;
use codex_tools::ToolSearchInfo;
use codex_tools::ToolSpec;
use codex_tools::coalesce_loadable_tool_specs;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::instrument;

pub struct ToolSearchHandler {
    search_infos: Vec<ToolSearchInfo>,
    spec: ToolSpec,
    search_engine: SearchEngine<usize>,
}

#[derive(Default)]
pub(crate) struct ToolSearchHandlerCache {
    cached: Mutex<Option<Arc<ToolSearchHandler>>>,
}

impl ToolSearchHandlerCache {
    #[instrument(level = "trace", skip_all, fields(search_info_count = search_infos.len()))]
    pub(crate) fn get_or_build(&self, search_infos: Vec<ToolSearchInfo>) -> Arc<ToolSearchHandler> {
        {
            let cached = self.cached();
            if let Some(cached) = cached.as_ref()
                && cached.search_infos == search_infos
            {
                return Arc::clone(cached);
            }
        }

        let handler = Arc::new(ToolSearchHandler::new(search_infos));
        let mut cached = self.cached();
        if let Some(cached) = cached.as_ref()
            && cached.search_infos == handler.search_infos
        {
            return Arc::clone(cached);
        }

        *cached = Some(Arc::clone(&handler));
        handler
    }

    fn cached(&self) -> std::sync::MutexGuard<'_, Option<Arc<ToolSearchHandler>>> {
        match self.cached.lock() {
            Ok(cached) => cached,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl ToolSearchHandler {
    #[instrument(
        level = "trace",
        skip_all,
        fields(search_info_count = search_infos.len())
    )]
    pub(crate) fn new(search_infos: Vec<ToolSearchInfo>) -> Self {
        let search_source_infos = search_infos
            .iter()
            .filter_map(|search_info| search_info.source_info.clone())
            .collect::<Vec<_>>();
        let spec = create_tool_search_tool(&search_source_infos, TOOL_SEARCH_DEFAULT_LIMIT);
        let documents: Vec<Document<usize>> = search_infos
            .iter()
            .map(|search_info| search_info.entry.search_text.clone())
            .enumerate()
            .map(|(idx, search_text)| Document::new(idx, search_text))
            .collect();
        let search_engine =
            SearchEngineBuilder::<usize>::with_documents(Language::English, documents).build();

        Self {
            search_infos,
            spec,
            search_engine,
        }
    }
}

impl ToolExecutor<ToolInvocation> for ToolSearchHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(TOOL_SEARCH_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        self.spec.clone()
    }

    fn supports_parallel_tool_calls(&self) -> bool {
        true
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(self.handle_call(invocation))
    }
}

impl ToolSearchHandler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation { payload, .. } = invocation;

        let args = match payload {
            ToolPayload::ToolSearch { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::Fatal(format!(
                    "{TOOL_SEARCH_TOOL_NAME} handler received unsupported payload"
                )));
            }
        };

        let query = args.query.trim();
        if query.is_empty() {
            return Err(FunctionCallError::RespondToModel(
                "query must not be empty".to_string(),
            ));
        }
        let limit = args.limit.unwrap_or(TOOL_SEARCH_DEFAULT_LIMIT);

        if limit == 0 {
            return Err(FunctionCallError::RespondToModel(
                "limit must be greater than zero".to_string(),
            ));
        }

        if self.search_infos.is_empty() {
            return Ok(boxed_tool_output(ToolSearchOutput { tools: Vec::new() }));
        }

        let tools = self.search(query, limit)?;

        Ok(boxed_tool_output(ToolSearchOutput { tools }))
    }
}

impl CoreToolRuntime for ToolSearchHandler {}

impl ToolSearchHandler {
    fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<LoadableToolSpec>, FunctionCallError> {
        let mut result_ids = self.exact_name_match_ids(query);
        let mut seen_ids = result_ids.iter().copied().collect::<HashSet<_>>();

        let remaining = limit.saturating_sub(result_ids.len());
        if remaining > 0 {
            result_ids.extend(
                self.search_engine
                    .search(query, self.search_infos.len())
                    .into_iter()
                    .map(|result| result.document.id)
                    .filter(|id| seen_ids.insert(*id))
                    .take(remaining),
            );
        }

        result_ids.truncate(limit);
        let results = result_ids
            .into_iter()
            .filter_map(|id| self.search_infos.get(id))
            .map(|search_info| &search_info.entry);
        self.search_output_tools(results)
    }

    fn exact_name_match_ids(&self, query: &str) -> Vec<usize> {
        let query_tokens = exact_match_query_tokens(query);
        if query_tokens.is_empty() {
            return Vec::new();
        }

        self.search_infos
            .iter()
            .enumerate()
            .filter_map(|(id, search_info)| {
                loadable_tool_spec_names(&search_info.entry.output)
                    .any(|name| {
                        query_tokens
                            .iter()
                            .any(|token| tool_name_matches_token(&name, token))
                    })
                    .then_some(id)
            })
            .collect()
    }

    fn search_output_tools<'a>(
        &self,
        results: impl IntoIterator<Item = &'a ToolSearchEntry>,
    ) -> Result<Vec<LoadableToolSpec>, FunctionCallError> {
        Ok(coalesce_loadable_tool_specs(
            results.into_iter().map(|entry| entry.output.clone()),
        ))
    }
}

fn exact_match_query_tokens(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn tool_name_matches_token(name: &str, token: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == token || name.ends_with(&format!("_{token}"))
}

fn loadable_tool_spec_names(spec: &LoadableToolSpec) -> impl Iterator<Item = String> + '_ {
    let mut names = Vec::new();
    match spec {
        LoadableToolSpec::Function(tool) => names.push(tool.name.clone()),
        LoadableToolSpec::Namespace(namespace) => {
            names.push(namespace.name.clone());
            for tool in &namespace.tools {
                let codex_tools::ResponsesApiNamespaceTool::Function(tool) = tool;
                names.push(tool.name.clone());
                names.push(format!("{}{}", namespace.name, tool.name));
            }
        }
    }
    names.into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::handlers::DynamicToolHandler;
    use crate::tools::handlers::McpHandler;
    use codex_mcp::ToolInfo;
    use codex_protocol::dynamic_tools::DynamicToolFunctionSpec;
    use codex_protocol::dynamic_tools::DynamicToolNamespaceSpec;
    use codex_tools::ResponsesApiNamespace;
    use codex_tools::ResponsesApiNamespaceTool;
    use codex_tools::ResponsesApiTool;
    use pretty_assertions::assert_eq;
    use rmcp::model::Tool;
    use std::sync::Arc;

    #[test]
    fn cache_reuses_handler_for_identical_search_infos_and_rebuilds_for_changes() {
        let cache = ToolSearchHandlerCache::default();
        let search_infos = vec![
            McpHandler::new(tool_info("calendar", "create_event", "Create events"))
                .expect("MCP tool should convert")
                .search_info()
                .expect("MCP handler should return search info"),
        ];

        let first = cache.get_or_build(search_infos.clone());
        let second = cache.get_or_build(search_infos.clone());
        assert!(Arc::ptr_eq(&first, &second));

        let mut changed_search_infos = search_infos;
        changed_search_infos[0]
            .entry
            .search_text
            .push_str(" changed");
        let changed = cache.get_or_build(changed_search_infos);
        assert!(!Arc::ptr_eq(&first, &changed));
    }

    #[test]
    fn mixed_search_results_coalesce_mcp_namespaces() {
        let dynamic_namespace = DynamicToolNamespaceSpec {
            name: "codex_app".to_string(),
            description: "Tools in the codex_app namespace.".to_string(),
            tools: Vec::new(),
        };
        let dynamic_tools = [DynamicToolFunctionSpec {
            name: "automation_update".to_string(),
            description: "Create, update, view, or delete recurring automations.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "mode": { "type": "string" },
                },
                "required": ["mode"],
                "additionalProperties": false,
            }),
            defer_loading: true,
        }];
        let mcp_tools = [
            tool_info("calendar", "create_event", "Create events"),
            tool_info("calendar", "list_events", "List events"),
        ];
        let mut search_infos = mcp_tools
            .iter()
            .map(|tool| {
                McpHandler::new(tool.clone())
                    .expect("MCP tool should convert")
                    .search_info()
                    .expect("MCP handler should return search info")
            })
            .collect::<Vec<_>>();
        search_infos.extend(dynamic_tools.iter().map(|tool| {
            DynamicToolHandler::new_in_namespace(&dynamic_namespace, tool)
                .expect("dynamic tool should convert")
                .search_info()
                .expect("dynamic handler should return search info")
        }));
        let handler = ToolSearchHandler::new(search_infos);
        let results = [
            &handler.search_infos[0].entry,
            &handler.search_infos[2].entry,
            &handler.search_infos[1].entry,
        ];

        let tools = handler
            .search_output_tools(results)
            .expect("mixed search output should serialize");

        assert_eq!(
            tools,
            vec![
                LoadableToolSpec::Namespace(ResponsesApiNamespace {
                    name: "mcp__calendar".to_string(),
                    description: "Tools in the mcp__calendar namespace.".to_string(),
                    tools: vec![
                        ResponsesApiNamespaceTool::Function(ResponsesApiTool {
                            name: "create_event".to_string(),
                            description: "Create events desktop tool".to_string(),
                            strict: false,
                            defer_loading: Some(true),
                            parameters: codex_tools::JsonSchema::object(
                                Default::default(),
                                /*required*/ None,
                                Some(false.into()),
                            ),
                            output_schema: None,
                        }),
                        ResponsesApiNamespaceTool::Function(ResponsesApiTool {
                            name: "list_events".to_string(),
                            description: "List events desktop tool".to_string(),
                            strict: false,
                            defer_loading: Some(true),
                            parameters: codex_tools::JsonSchema::object(
                                Default::default(),
                                /*required*/ None,
                                Some(false.into()),
                            ),
                            output_schema: None,
                        }),
                    ],
                }),
                LoadableToolSpec::Namespace(ResponsesApiNamespace {
                    name: "codex_app".to_string(),
                    description: "Tools in the codex_app namespace.".to_string(),
                    tools: vec![ResponsesApiNamespaceTool::Function(ResponsesApiTool {
                        name: "automation_update".to_string(),
                        description: "Create, update, view, or delete recurring automations."
                            .to_string(),
                        strict: false,
                        defer_loading: Some(true),
                        parameters: codex_tools::JsonSchema::object(
                            std::collections::BTreeMap::from([(
                                "mode".to_string(),
                                codex_tools::JsonSchema::string(/*description*/ None),
                            )]),
                            Some(vec!["mode".to_string()]),
                            Some(false.into()),
                        ),
                        output_schema: None,
                    })],
                }),
            ],
        );
    }

    #[test]
    fn search_promotes_exact_tool_name_matches_before_bm25_results() {
        let mcp_tools = [
            tool_info(
                "ironclaw_workflow",
                "workflow_run_dispatch_round",
                "Coordinate the next workflow round",
            ),
            tool_info(
                "ironclaw_workflow",
                "workflow_run_complete",
                "dispatch_round dispatch_round dispatch_round complete workflow",
            ),
            tool_info(
                "ironclaw_workflow",
                "workflow_run_spawn",
                "dispatch_round dispatch_round dispatch_round spawn workflow",
            ),
        ];
        let search_infos = mcp_tools
            .iter()
            .map(|tool| {
                McpHandler::new(tool.clone())
                    .expect("MCP tool should convert")
                    .search_info()
                    .expect("MCP handler should return search info")
            })
            .collect::<Vec<_>>();
        let handler = ToolSearchHandler::new(search_infos);

        let tools = handler
            .search("dispatch_round", /*limit*/ 1)
            .expect("search should succeed");

        assert_eq!(
            tools,
            vec![LoadableToolSpec::Namespace(ResponsesApiNamespace {
                name: "mcp__ironclaw_workflow".to_string(),
                description: "Tools in the mcp__ironclaw_workflow namespace.".to_string(),
                tools: vec![ResponsesApiNamespaceTool::Function(ResponsesApiTool {
                    name: "workflow_run_dispatch_round".to_string(),
                    description: "Coordinate the next workflow round desktop tool".to_string(),
                    strict: false,
                    defer_loading: Some(true),
                    parameters: codex_tools::JsonSchema::object(
                        Default::default(),
                        /*required*/ None,
                        Some(false.into()),
                    ),
                    output_schema: None,
                })],
            })]
        );
    }

    #[test]
    fn search_matches_a_full_tool_name_case_insensitively() {
        let handler = ToolSearchHandler::new(vec![
            McpHandler::new(tool_info(
                "ironclaw_workflow",
                "workflow_run_dispatch_round",
                "Coordinate the next workflow round",
            ))
            .expect("MCP tool should convert")
            .search_info()
            .expect("MCP handler should return search info"),
        ]);

        let tools = handler
            .search("WORKFLOW_RUN_DISPATCH_ROUND", /*limit*/ 1)
            .expect("search should succeed");

        assert_eq!(loadable_tool_names(&tools), ["workflow_run_dispatch_round"]);
    }

    #[test]
    fn exact_name_matching_finds_a_tool_identifier_embedded_in_prose() {
        let handler = ToolSearchHandler::new(vec![
            McpHandler::new(tool_info(
                "ironclaw_workflow",
                "workflow_run_dispatch_round",
                "Coordinate the next workflow round",
            ))
            .expect("MCP tool should convert")
            .search_info()
            .expect("MCP handler should return search info"),
        ]);

        assert_eq!(
            handler.exact_name_match_ids("Please call `WORKFLOW_RUN_DISPATCH_ROUND` next."),
            [0]
        );
    }

    #[test]
    fn search_matches_the_runtime_flattened_namespaced_tool_name() {
        let handler = ToolSearchHandler::new(vec![
            McpHandler::new(tool_info(
                "ironclaw_workflow",
                "workflow_run_dispatch_round",
                "Coordinate the next workflow round",
            ))
            .expect("MCP tool should convert")
            .search_info()
            .expect("MCP handler should return search info"),
        ]);

        let tools = handler
            .search(
                "mcp__ironclaw_workflowworkflow_run_dispatch_round",
                /*limit*/ 1,
            )
            .expect("search should succeed");

        assert_eq!(loadable_tool_names(&tools), ["workflow_run_dispatch_round"]);
    }

    #[test]
    fn search_prioritizes_all_exact_leaf_matches_before_bm25_and_respects_limit() {
        let mcp_tools = [
            tool_info("workflow", "first_dispatch_round", "first exact match"),
            tool_info("workflow", "second_dispatch_round", "second exact match"),
            tool_info(
                "workflow",
                "unrelated_tool",
                "dispatch_round dispatch_round dispatch_round",
            ),
        ];
        let handler = ToolSearchHandler::new(
            mcp_tools
                .iter()
                .map(|tool| {
                    McpHandler::new(tool.clone())
                        .expect("MCP tool should convert")
                        .search_info()
                        .expect("MCP handler should return search info")
                })
                .collect(),
        );

        let tools = handler
            .search("dispatch_round", /*limit*/ 2)
            .expect("search should succeed");

        assert_eq!(
            loadable_tool_names(&tools),
            ["first_dispatch_round", "second_dispatch_round"]
        );
    }

    #[test]
    fn search_backfills_exact_results_with_distinct_bm25_results() {
        let mcp_tools = [
            tool_info("workflow", "workflow_run_dispatch_round", "exact match"),
            tool_info(
                "workflow",
                "workflow_run_complete",
                "dispatch_round dispatch_round dispatch_round",
            ),
        ];
        let handler = ToolSearchHandler::new(
            mcp_tools
                .iter()
                .map(|tool| {
                    McpHandler::new(tool.clone())
                        .expect("MCP tool should convert")
                        .search_info()
                        .expect("MCP handler should return search info")
                })
                .collect(),
        );

        let tools = handler
            .search("dispatch_round", /*limit*/ 2)
            .expect("search should succeed");

        assert_eq!(
            loadable_tool_names(&tools),
            ["workflow_run_dispatch_round", "workflow_run_complete"]
        );
    }

    #[test]
    fn exact_name_matching_does_not_promote_partial_tokens() {
        assert!(tool_name_matches_token(
            "workflow_run_dispatch_round",
            "dispatch_round"
        ));
        assert!(!tool_name_matches_token(
            "workflow_run_dispatch_round",
            "dispatch"
        ));
        assert!(!tool_name_matches_token(
            "workflow_run_dispatch_round",
            "rounds"
        ));
    }

    fn loadable_tool_names(specs: &[LoadableToolSpec]) -> Vec<&str> {
        specs
            .iter()
            .flat_map(|spec| match spec {
                LoadableToolSpec::Function(tool) => vec![tool.name.as_str()],
                LoadableToolSpec::Namespace(namespace) => namespace
                    .tools
                    .iter()
                    .map(|tool| {
                        let ResponsesApiNamespaceTool::Function(tool) = tool;
                        tool.name.as_str()
                    })
                    .collect(),
            })
            .collect()
    }

    fn tool_info(server_name: &str, tool_name: &str, description_prefix: &str) -> ToolInfo {
        ToolInfo {
            server_name: server_name.to_string(),
            supports_parallel_tool_calls: false,
            server_origin: None,
            callable_name: tool_name.to_string(),
            callable_namespace: format!("mcp__{server_name}"),
            namespace_description: None,
            tool: Tool::new(
                tool_name.to_string(),
                format!("{description_prefix} desktop tool"),
                Arc::new(rmcp::model::object(serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                }))),
            ),
            openai_file_input_optional_fields: Default::default(),
            connector_id: None,
            connector_name: None,
            plugin_display_names: Vec::new(),
        }
    }
}
