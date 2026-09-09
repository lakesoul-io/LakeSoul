发现的一个上游 quirk（与后续统一 settings 相关）

 验证时发现：上游 SetShowHook 处理 SET TIME ZONE 时对
 session_context.state() 的返回值（快照）调 config_mut()，不会改动活
 context 的配置，只更新 client metadata / ParameterStatus；而 SET
 datafusion.execution.time_zone = ... 会经 execute_set_statement →
 set_variable 真正落到活 context。这印证了你提到的 “PgSession.settings
 与 SetShowHook client metadata 双重状态源” 问题，建议后续统一时一并修掉
 。