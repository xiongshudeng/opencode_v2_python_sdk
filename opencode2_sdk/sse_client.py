"""
SSE (Server-Sent Events) 客户端模块。

提供异步事件流处理功能，用于接收服务器推送的实时事件。
"""

import json
import asyncio
from typing import AsyncIterator, Optional, Dict, Any
import httpx
from httpx._types import AuthTypes

from .models.events import Event, EventSessionCommon
from .exceptions import ConnectionError, TimeoutError, APIError


class SSEClient:
    """
    SSE (Server-Sent Events) 客户端。
    
    用于处理服务器推送的事件流，支持异步迭代。
    """

    def __init__(
            self,
            base_url: str,
            headers: Optional[Dict[str, str]] = None,
            timeout: Optional[float] = None,
            auth: Optional[AuthTypes] = None
    ):
        """
        初始化 SSE 客户端。
        
        Args:
            base_url: API 基础 URL
            headers: 请求头
            timeout: 超时时间（秒）
        """
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = timeout
        self.auth = auth
        self._client: Optional[httpx] = None

    async def __aenter__(self):
        """异步上下文管理器入口。"""
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self.headers,
            timeout=self.timeout,
            auth=self.auth,
        )
        return self

    async def __aexit__(self, *args):
        """异步上下文管理器退出。"""
        if self._client:
            await self._client.aclose()

    async def connect(
            self,
            url: str,
            params: Optional[Dict[str, Any]] = None,
            method: str = "GET",
            json_data: Optional[Dict[str, Any]] = None
    ) -> AsyncIterator[Event]:
        """
        连接到 SSE 端点并接收事件流。
        
        Args:
            url: SSE 端点 URL（相对路径）
            params: 查询参数（用于 GET 请求）
            method: HTTP 方法（GET 或 POST）
            json_data: JSON 数据（用于 POST 请求）
            
        Yields:
            Event 对象
            
        Raises:
            ConnectionError: 连接失败
            TimeoutError: 连接超时
            APIError: API 错误
            
        Example:
            >>> async with SSEClient(base_url="http://localhost:8000") as client:
            ...     async for event in client.connect("/global/event"):
            ...         print(f"收到事件: {event.type}")
        """
        if not self._client:
            raise RuntimeError("SSEClient 必须在 async with 语句中使用")

        try:
            # 根据方法类型构建请求
            request_kwargs = {
                "headers": {"Accept": "text/event-stream"},
                "timeout": None  # SSE 连接不应该有超时
            }

            if method.upper() == "POST":
                request_kwargs["json"] = json_data
            else:
                request_kwargs["params"] = params

            #print("sse url:", url, ",kwargs:", request_kwargs)

            # with PYSSEClient("http://localhost:4096" + url, auth=('opencode','chatmyppt-1234')) as response:
            # while True:
            #    for msg in response:
            #        handle_message(msg)
            async with self._client.stream(
                    method,
                    url,
                    **request_kwargs
            ) as response:
                if response.status_code == 204:
                    # 204 表示请求成功但没有流式内容，直接返回
                    return
                elif response.status_code != 200:
                    error_text = await response.aread()
                    raise APIError(
                        message=f"SSE 连接失败: {response.status_code}",
                        status_code=response.status_code,
                        response_body=error_text.decode()
                    )

                # 处理事件流
                async for event in self._parse_stream(response):
                    #print("sse yield event")
                    yield event
            # print("sse yield out")
        except httpx.TimeoutException as e:
            raise TimeoutError(f"SSE 连接超时: {str(e)}")
        except httpx.ConnectError as e:
            raise ConnectionError(f"SSE 连接失败: {str(e)}")
        except Exception as e:
            if isinstance(e, (APIError, TimeoutError, ConnectionError)):
                raise
            raise APIError(f"SSE 处理错误: {str(e)}")

    async def _parse_stream(self, response: httpx.Response) -> AsyncIterator[Event]:
        """
        解析 SSE 事件流。
        
        Args:
            response: HTTP 响应对象
            
        Yields:
            Event 对象
        """
        event_type: Optional[str] = None
        event_data: str = ""
        # print("parse stream:", response)
        async for line in response.aiter_lines():
            line = line.strip()
            #print("line00:", line)
            # 空行表示事件结束
            if not line:
                #print("not line")
                if event_data:
                    #print("line: event_data:", event_data, ",type:", event_type)
                    # 解析并生成事件（event_type 可能为 None，从 data 中获取）
                    event = self._parse_event(event_type, event_data)
                    #print("line: event:", event)
                    if event:
                        #print("line: yield event:", event)
                        yield event

                    # 重置状态
                    event_type = None
                    event_data = ""
                continue

            # 解析事件字段
            # opencode v2 event变type
            if line.startswith("event:"):
                event_type = line[6:].strip()
                #print("event line:", event_type)
            elif line.startswith("type:"):
                event_type = line[5:].strip()
                #print("type line:", event_type)
            elif line.startswith("data:"):
                data = line[5:].strip()
                #print("data line:", data)
                if event_data:
                    event_data += "\n" + data
                    #print("data line add event:", event_data)
                else:
                    event_data = data
                    #print("line: set event_data", event_data)
                #print("data line:", line)
            # 忽略其他字段（id, retry 等）
            #print("parse stream end")
        #print("parse stream out")

    def _parse_event(self, event_type: Optional[str], event_data: str) -> Optional[Event]:
        """
        解析单个事件。
        
        Args:
            event_type: 事件类型（可能为 None，从 data 中获取）
            event_data: 事件数据（JSON 字符串）
            
        Returns:
            Event 对象，如果解析失败则返回 None
        """
        try:
            # 解析 JSON 数据
            data = json.loads(event_data)
            #print("event json data:", data)
            # 如果 data 不是字典，返回 None
            if not isinstance(data, dict):
                return None

            # 检查是否有 payload 字段（服务器可能将事件包装在 payload 中）
            #if "payload" in data and isinstance(data["payload"], dict):
            #    data = data["payload"]

            # 获取事件类型
            if event_type is None:
                event_type = data.get("type")
            #print("get event type:", event_type)
            if not event_type:
                return None
            #print("map event")
            # 根据事件类型导入对应的类
            from .models.events import (
                EventServerConnected,
                EventServerInstanceDisposed,
                EventSessionStatus,
                EventSessionIdle,
                EventSessionCreated,
                EventSessionUpdated,
                EventSessionDeleted,
                EventSessionDiff,
                EventSessionError,
                EventSessionCompacted,
                EventMessageUpdated,
                EventMessageRemoved,
                EventMessagePartUpdated,
                EventMessagePartRemoved,
                EventMessagePartDelta,
                EventPermissionUpdated,
                EventPermissionReplied,
                EventFileEdited,
                EventFileWatcherUpdated,
                EventVcsBranchUpdated,
                EventTodoUpdated,
                EventCommandExecuted,
                EventTuiPromptAppend,
                EventTuiCommandExecute,
                EventTuiToastShow,
                EventPtyCreated,
                EventPtyUpdated,
                EventPtyExited,
                EventPtyDeleted,
                EventInstallationUpdated,
                EventInstallationUpdateAvailable,
                EventLspClientDiagnostics,
                EventLspUpdated,
                EventSessionTextDelta,
                EventSessionReasoningDelta,
                EventSessionReasoningEnded,
                EventSessionTextEnded,
            )

            # 事件类型映射
            event_class_map = {
                "server.connected": EventServerConnected,
                "server.instance.disposed": EventServerInstanceDisposed,
                "session.status": EventSessionStatus,
                "session.idle": EventSessionIdle,
                "session.created": EventSessionCreated,
                "session.updated": EventSessionUpdated,
                "session.deleted": EventSessionDeleted,
                "session.diff": EventSessionDiff,
                "session.error": EventSessionError,
                "session.compacted": EventSessionCompacted,
                "message.updated": EventMessageUpdated,
                "message.removed": EventMessageRemoved,
                "message.part.updated": EventMessagePartUpdated,
                "message.part.removed": EventMessagePartRemoved,
                "message.part.delta": EventMessagePartDelta,
                "permission.updated": EventPermissionUpdated,
                "permission.replied": EventPermissionReplied,
                "file.edited": EventFileEdited,
                "file.watcher.updated": EventFileWatcherUpdated,
                "vcs.branch.updated": EventVcsBranchUpdated,
                "todo.updated": EventTodoUpdated,
                "command.executed": EventCommandExecuted,
                "tui.prompt.append": EventTuiPromptAppend,
                "tui.command.execute": EventTuiCommandExecute,
                "tui.toast.show": EventTuiToastShow,
                "pty.created": EventPtyCreated,
                "pty.updated": EventPtyUpdated,
                "pty.exited": EventPtyExited,
                "pty.deleted": EventPtyDeleted,
                "installation.updated": EventInstallationUpdated,
                "installation.update-available": EventInstallationUpdateAvailable,
                "lsp.client.diagnostics": EventLspClientDiagnostics,
                "lsp.updated": EventLspUpdated,
                "session.text.delta": EventSessionTextDelta,
                "session.reasoning.delta": EventSessionReasoningDelta,
                "session.reasoning.ended": EventSessionReasoningEnded,
                "session.text.ended": EventSessionTextEnded,
            }
            event_type_map = ["session.text.delta", "session.reasoning.delta",
                               "session.reasoning.ended", "session.text.ended",
                               "session.step.ended", "session.execution.succeeded",
                               '"permission.asked"'
            ]
            # 获取对应的事件类
            #event_class = event_class_map.get(event_type)
            #print("map event_type:", event_type)
            if event_type in event_type_map:
                # try:
                event_data = data['data']
                #print("event data:", event_data)
                # Correct
                # item = event_class.model_validate(event_data)
                # print("validata")
                #print("init event common,event_type:", event_type)
                session_id = event_data.get('sessionID') or None
                #print("session_id:", session_id)
                asid = event_data.get('assistantMessageID') or None
                #print("assit id:", asid)
                if 'delta' in event_data:
                    delta_str = event_data.get('delta')
                else:
                    delta_str = 'no'
                #print("delta str:", delta_str)
                if 'text' in event_data:
                    text_str = event_data.get('text')
                else:
                    text_str = 'no'
                #print("text str:", text_str)
                #print("start init class,delta:", delta_str, ",text:", text_str)
                e_c = EventSessionCommon(type_str=event_type, sessionID=session_id, assistantMessageID=asid,
                                         delta_str=delta_str, text_str=text_str)
                #print("event data out ok,ec:", e_c)
                return e_c
            elif event_type in ['form.created']:
                event_data = data['data']
                if event_data:
                    event_form = event_data.get('form')
                    if event_form:
                        session_id = event_form.get('sessionID') or None
                        #print("session_id:", session_id)
                        asid = event_form.get('assistantMessageID') or None
                        title = event_form.get('title')
                        metadata = event_form.get('metadata')
                        e_c = EventSessionCommon(type_str=event_type, sessionID=session_id, assistantMessageID=asid,
                                                 delta_str='no', text_str=json.dumps(event_data))
                        # print("event data out ok,ec:", e_c)
                        return e_c
            elif event_type in ['session.tool.success']:
                event_data = data['data']
                #print("event data:", event_data)
                session_id = event_data.get('sessionID') or None
                #print("session_id:", session_id)
                asid = event_data.get('assistantMessageID') or None
                content = event_data.get("content")
                text_str = ''
                for item in content:
                    text_str += item.get("text")
                #print("tool success text:", text_str)
                e_c = EventSessionCommon(type_str=event_type, sessionID=session_id, assistantMessageID=asid,
                                         delta_str='no', text_str=text_str)
                # print("event data out ok,ec:", e_c)
                return e_c
            else:
                #print("event return None")
                # 未知事件类型，静默忽略
                return None

        except json.JSONDecodeError:
            # JSON 解析失败，静默忽略
            print("json decode fail")
            return None
        except Exception:
            # 其他错误，静默忽略
            print("other exception")
            return None


async def subscribe_events(
        base_url: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None
) -> AsyncIterator[Event]:
    """
    订阅事件流的便捷函数。
    
    Args:
        base_url: API 基础 URL
        url: SSE 端点 URL
        headers: 请求头
        params: 查询参数
        timeout: 超时时间
        
    Yields:
        Event 对象
        
    Example:
        >>> async for event in subscribe_events(
        ...     base_url="http://localhost:8000",
        ...     url="/global/event"
        ... ):
        ...     print(f"收到事件: {event.type}")
    """
    async with SSEClient(base_url, headers, timeout) as client:
        async for event in client.connect(url, params):
            yield event
