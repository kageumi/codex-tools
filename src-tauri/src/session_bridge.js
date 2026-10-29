// Runs only in the connected Codex renderer. All storage operations go through
// that desktop's existing local app-server connection, never through a second CLI.
(() => {
  const key = "__codexToolsSessionsV1";
  const version = 4;
  if (!window.electronBridge?.sendMessageFromView) throw Error("Codex 桌面桥接不可用。");
  if (window[key]?.version === version) return true;
  if (window[key] && typeof window[key].dispose !== "function") throw Error("旧版会话同步脚本需要完全退出并重启 Codex 后再连接。");
  window[key]?.dispose();
  const sourceKinds = ["cli", "vscode", "exec", "appServer", "subAgent", "subAgentReview", "subAgentCompact", "subAgentThreadSpawn", "subAgentOther", "unknown"];
  const request = (method, params) => new Promise((resolve, reject) => {
    const id = crypto.randomUUID();
    const listener = (event) => {
      const message = event.data;
      if (message?.type !== "mcp-response" || message.message?.id !== id) return;
      clearTimeout(timer);
      window.removeEventListener("message", listener);
      const response = message.message;
      if (response.error) reject(Error(response.error.message || "Codex 会话操作失败。"));
      else resolve(response.result);
    };
    const timer = setTimeout(() => {
      window.removeEventListener("message", listener);
      reject(Error("等待 Codex 响应超时；请核对会话状态后再操作。"));
    }, 25000);
    window.addEventListener("message", listener);
    window.electronBridge.sendMessageFromView({type: "mcp-request", hostId: "local", request: {id, method, params}, timeoutMs: 25000});
  });
  const summarize = (thread, archived) => ({
    id: thread.id, title: thread.name || thread.title || thread.preview || "未命名会话",
    fallbackTitle: thread.preview || thread.title || "未命名会话",
    preview: thread.preview,
    path: thread.path, status: thread.status,
    cwd: thread.cwd, provider: thread.modelProvider, updatedAt: thread.updatedAt,
    parent: (typeof thread.parentThreadId === 'string' ? thread.parentThreadId : null) || thread.source?.subAgent?.thread_spawn?.parent_thread_id || thread.source?.subagent?.thread_spawn?.parent_thread_id || null,
    archived,
  });
  const known = new Map();
  const list = async (kinds = sourceKinds) => {
    const output = [];
    for (const archived of [false, true]) {
      let cursor = null;
      const seen = new Set();
      do {
        const page = await request("thread/list", {limit: 100, cursor, archived, sourceKinds: kinds, modelProviders: []});
        for (const thread of page.data || []) {
          const summary = summarize(thread, archived);
          known.set(summary.id, summary);
          output.push(summary);
        }
        cursor = page.nextCursor;
        if (cursor && seen.has(cursor)) throw Error("Codex 会话分页游标重复。");
        seen.add(cursor);
      } while (cursor);
    }
    return output;
  };
  const affectedScope = async (threadId, onlyRoot = false) => {
    const fresh = new Map();
    const reads = new Map();
    const read = (id) => {
      if (!reads.has(id)) reads.set(id, request("thread/read", {threadId:id, includeTurns:false}).then(result => {
        if (!result.thread || result.thread.id !== id) throw Error("Codex 会话元数据无效。");
        const summary = summarize(result.thread, known.get(id)?.archived || false);
        known.set(id, summary);
        fresh.set(id, summary);
        return summary;
      }));
      return reads.get(id);
    };
    if (!onlyRoot) {
      for (const child of await list(['subAgentThreadSpawn'])) fresh.set(child.id, child);
    }
    const loaded = new Set((await request("thread/loaded/list", {})).data || []);
    if (!onlyRoot) {
      // Empty loaded threads may be omitted from the native list. Reuse known
      // parents and read only missing metadata, following ancestors without cycles.
      for (const loadedId of loaded) {
        let id = loadedId;
        const visited = new Set();
        while (id && id !== threadId && !visited.has(id)) {
          visited.add(id);
          const thread = known.get(id) || await read(id);
          id = thread.parent;
        }
      }
    }
    const actual = new Set([threadId]);
    if (!onlyRoot) {
      let size;
      do { size=actual.size; for(const child of known.values()) if(actual.has(child.parent)) actual.add(child.id); } while(size!==actual.size);
    }
    let blockedReason = null;
    for (const id of actual) {
      if (id !== threadId && !loaded.has(id)) continue;
      const thread = fresh.get(id) || await read(id);
      const type = thread.status?.type;
      if (type === 'active') blockedReason = "会话或派生会话正在运行，请等待完成后再操作。";
      else if (!['idle','notLoaded'].includes(type) && !blockedReason) blockedReason = "无法确认会话运行状态，暂不能操作。";
    }
    return {threads:[...actual].map(id => fresh.get(id) || known.get(id)), blockedReason};
  };
  const guardAndMutate = async (method, threadId, affectedIds) => {
    // Check immediately before mutation, including loaded but archived descendants.
    // Native archive/delete may shut down a live thread, so do not rely on them to reject it.
    const affected = new Set([threadId, ...affectedIds]);
    let changed = null;
    const guardListener = (event) => {
      const message = event.data;
      if (message?.type !== 'mcp-notification' || message.hostId !== 'local') return;
      if (message.method === 'thread/status/changed' && affected.has(message.params?.threadId) && message.params.status?.type === 'active') changed = '会话或派生会话已开始运行，请等待完成后再操作。';
      if (message.method === 'thread/started') {
        const thread = summarize(message.params.thread, false);
        if (thread.parent && affected.has(thread.parent)) changed = '派生会话范围已变化，请重新确认操作范围。';
      }
    };
    window.addEventListener('message', guardListener);
    try {
      const scope = await affectedScope(threadId, method === 'thread/unarchive');
      if (scope.blockedReason) throw Error(scope.blockedReason);
      const actual = new Set(scope.threads.map(thread => thread.id));
      if(actual.size!==affected.size || [...actual].some(id=>!affected.has(id))) throw Error('派生会话范围已变化，请重新确认操作范围。');
      if (changed) throw Error(changed);
    } catch (error) {
      return {dispatched: false, error: error.message};
    } finally { window.removeEventListener('message',guardListener); }
    try { return {dispatched:true, value:await request(method, {threadId})}; }
    catch (error) { return {dispatched:true, error:error.message}; }
  };
  const watched = new Set(["thread/started", "thread/archived", "thread/unarchived", "thread/deleted", "thread/status/changed", "thread/name/updated", "turn/started", "turn/completed"]);
  const notificationListener = (event) => {
    const message = event.data;
    if (message?.type !== "mcp-notification" || message.hostId !== "local" || !watched.has(message.method)) return;
    const params = message.params || {};
    const data = {method: message.method, threadId: params.threadId, status: params.status};
    if (message.method === "thread/name/updated" && (typeof params.name === "string" || params.name === null)) data.name = params.name;
    if (params.thread) {
      data.thread = summarize(params.thread, false);
      known.set(data.thread.id, data.thread);
    }
    const current = known.get(params.threadId);
    if (message.method === 'thread/deleted') known.delete(params.threadId);
    if (current) {
      if (message.method === 'thread/status/changed') current.status = params.status;
      if (message.method === 'thread/archived') current.archived = true;
      if (message.method === 'thread/unarchived') current.archived = false;
      if (message.method === 'thread/name/updated') current.title = params.name || current.fallbackTitle;
    }
    window.__codexToolsSessionEvent(JSON.stringify(data));
  };
  window.addEventListener("message", notificationListener);
  window[key] = {request, list, affectedScope, guardAndMutate, version, dispose: () => window.removeEventListener("message", notificationListener)};
  return true;
})()
