"""
Event 资源模块。

提供事件订阅功能，用于接收服务器推送的实时事件。
"""

from typing import AsyncIterator, Optional, Dict, Any

from httpx._types import AuthTypes

from ..models.events import Event, GlobalEvent
from ..sse_client import SSEClient
from .base import BaseResource


class EventResource(BaseResource):
    """
    Event 资源类。
    
    提供事件订阅功能，支持：
    - 全局事件订阅
    - 会话事件订阅
    - 实时事件流处理
    """
    
    async def subscribe(
        self,
        session_id: Optional[str] = None,
        **kwargs
    ) -> AsyncIterator[Event]:
        """
        订阅事件流。
        
        如果提供 session_id，则订阅特定会话的事件；
        否则订阅全局事件。
        
        Args:
            session_id: 可选的会话 ID
            **kwargs: 其他查询参数
            
        Yields:
            Event 对象
            
        Raises:
            ConnectionError: 连接失败
            TimeoutError: 连接超时
            APIError: API 错误
            
        Example:
            >>> # 订阅全局事件
            >>> async for event in client.events.subscribe():
            ...     print(f"全局事件: {event.type}")
            
            >>> # 订阅会话事件
            >>> async for event in client.events.subscribe(session_id="session_123"):
            ...     if event.type == "text":
            ...         print(event.text, end="", flush=True)
        """
        # 构建 URL
        if session_id:
            url = f"/session/{session_id}/prompt_async"
        else:
            url = "/global/event"
        
        # 构建查询参数
        params = dict(kwargs)
        print("only client sessionid url:", url, ",base url:",self._http_client.base_url)
        # 创建 SSE 客户端
        async with SSEClient(
            base_url=self._http_client.base_url,
            headers=self._http_client.default_headers,
            timeout=self._http_client.timeout,
            auth=self._http_client.auth
        ) as sse_client:
            print("only session connect url:",url)
            async for event in sse_client.connect(url, params):
                print("only sessionid get event:", event)
                yield event
    
    async def subscribe_global(self) -> AsyncIterator[GlobalEvent]:
        """
        订阅全局事件。
        
        这是 subscribe() 的便捷方法，专门用于订阅全局事件。
        
        Yields:
            GlobalEvent 对象
            
        Example:
            >>> async for event in client.events.subscribe_global():
            ...     print(f"全局事件: {event.type}")
            ...     if event.type == "session:created":
            ...         print(f"新会话: {event.info.name}")
        """
        async for event in self.subscribe():
            # 全局事件应该是 GlobalEvent 类型
            if isinstance(event, dict):
                yield GlobalEvent(**event)
            else:
                yield event
    
    async def subscribe_session(
        self,
        session_id: str,
        text: str,
        **kwargs
    ) -> AsyncIterator[Event]:
        """
        订阅会话事件（发送消息并接收响应流）。
        
        正确的流程：
        1. 发送消息到 /session/{id}/prompt_async（触发任务）
        2. 订阅 /event 端点（SSE 流）接收响应
        
        Args:
            session_id: 会话 ID
            text: 消息部分列表（如果提供，则发送消息）
            **kwargs: 其他参数（如 model, agent 等）
            
        Yields:
            Event 对象
            
        Example:
            >>> async for event in client.events.subscribe_session(
            ...     session_id="session_123",
            ...     text="你好",
            ...     agent="build"
            ... ):
            ...     if event.type == "text":
            ...         print(event.text, end="", flush=True)
        """
        print("subscribe_session:", session_id)
        # 如果提供了 parts，需要先发送消息
        print("text:", text)
        if text:
            # 构建请求数据
            data = {'text': text}
            #data = {'prompt': text}
            #data = {"parts": parts}
            #print("subscript data:", data)
            # 将 kwargs 中的参数添加到 data
            for key in ['agents']:
                if key in kwargs:
                    data[key] = kwargs[key]
            data['delivery'] = 'steer'
            # 构建 prompt_async 的查询参数
            prompt_params = {}
            
            # 先发送消息到 prompt_async 端点（触发任务）
            prompt_url = f"/api/session/{session_id}/prompt"
            #prompt_url = f"/api/session/{session_id}/prompt_async"
            #print("client post:", prompt_url)
            # 发送 POST 请求
            resp_json = self._http_client.post(
                prompt_url,
                json_data=data,
                params=prompt_params
            )
            #print("post prompt resp json:", resp_json)
            # 发送 POST 请求
            #data = {'prompt': "改写前面的文本，让文本不像AI写的"}
            #prompt_url = f"/api/session/{session_id}/Generate"
            #resp_json = self._http_client.post(
            #    prompt_url,
            #    json_data=data,
            #    params=prompt_params
            #)
            #print("post generate prompt resp json:",resp_json)
            #print("client post out :", prompt_url)
            #print("post data:", data)
            #print("prompt_params", prompt_params)
            # 然后订阅事件流
            event_params = {}
            url = "/api/event"
            # 使用 SSE 客户端订阅事件
            #print("create event client")
            async with SSEClient(
                base_url=self._http_client.base_url,
                headers=self._http_client.default_headers,
                timeout=self._http_client.timeout,
                auth=self._http_client.auth
            ) as sse_client:
                # 启动事件流连接（GET 请求，带 directory 参数）
                print("sse event post url:", url, ",base_url:", self._http_client.base_url)
                async for event in sse_client.connect(url, params=event_params, method="GET"):
                    #print("sse get event type:", event.type)
                    # 检查是否是当前会话
                    if hasattr(event, 'properties') and ('session_id' in event.properties):
                        #print("property session_id", event.properties['session_id'])
                        if event.properties['session_id'] == session_id:
                            #只返回当前的seesion 事件
                            #print("yield event sessionid:", session_id)
                            yield event

                    # 如果收到 session.idle 事件，表示会话完成
                    #print("get event idle,type:", event.type)
                    if hasattr(event, 'type') and event.type == "session.execution.succeeded":
                        print("session.idle execution.succeeded")
                        #print(event)
                        # 检查是否是当前会话
                        if hasattr(event, 'properties') and ('session_id' in event.properties):
                            print("property session_id:", session_id)
                            #print(event)
                            if event.properties['session_id'] == session_id:
                                print("event idle break,session:", session_id)
                                break
                    #print("get event loop next,type:", event.type)
                #print("get event out:", event)
        else:
            print("only subscrbe event:", session_id)
            # 只订阅事件，不发送消息
            async for event in self.subscribe(session_id=session_id,  **kwargs):
                print("np parts event")
                yield event
                # 如果收到 session.idle 事件，表示会话完成
                if hasattr(event, 'type') and event.type == "session.idle":
                    break

    def post_session_confirm(
        self,
        session_id: str,
        text: str,
        **kwargs
    ):
        if text:
            # 构建请求数据
            data = {'text': text}
            # 将 kwargs 中的参数添加到 data
            for key in ['agent', 'model', 'variant']:
                if key in kwargs:
                    data[key] = kwargs[key]

            # 构建 prompt_async 的查询参数
            prompt_params = {}

            # 先发送消息到 prompt_async 端点（触发任务）
            prompt_url = f"/api/session/{session_id}/prompt"

            # 发送 POST 请求
            self._http_client.post(
                prompt_url,
                json_data=data,
                params=prompt_params
            )
            print("post confirm url:", prompt_url, data)
    def post_session_init(
        self,
        session_id: str,
        parts: Optional[list] = None,
        directory: Optional[str] = None,
        **kwargs
    ):
        if parts:
            # 构建请求数据
            data = {"parts": parts}
            # 将 kwargs 中的参数添加到 data
            for key in ['agent', 'model', 'variant']:
                if key in kwargs:
                    data[key] = kwargs[key]

            # 构建 prompt_async 的查询参数
            prompt_params = {}
            if directory:
                prompt_params['directory'] = directory

            # 先发送消息到 prompt_async 端点（触发任务）
            prompt_url = f"/session/{session_id}/init"
            print("post session init:", prompt_url, data)
            # 发送 POST 请求
            self._http_client.post(
                prompt_url,
                json_data=data,
                params=prompt_params
            )
            print("post session init ok")