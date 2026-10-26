import asyncio
import shutil
from pathlib import Path
import datetime
from typing import Optional

from opencode2_sdk import Opencode2Client

PPT_MAC_BASE_DIR = '/Users/xiongshudeng/Downloads/myskills2'
BASE_DIR = PPT_MAC_BASE_DIR

OPENCODE_BASE_URL = "http://127.0.0.1:4096"
# OPENCODE_BASE_URL = "http://127.0.0.1:49374"
#OPENCODE_SERVER_USERNAME = 'your name'
#opencode自定义文件名没有调通,默认opencode
OPENCODE_SERVER_USERNAME = 'opencode'
#opencode可以设置密码启动，如果不设置密码，opencode serve启动的时候会打印出密码
#OPENCODE_SERVER_PASSWORD=3412-1234 opencode serve --log-level all
OPENCODE_SERVER_PASSWORD = '3412-1234'
session_data = {}
# 全局订阅者集合


def form_reply(mysessionid, formid, form_answer: str):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} log design form")
    client = session_data[mysessionid]
    if client is None:
        return
    print("form new client session id:", mysessionid)
    session = client.sessions.get(mysessionid)
    sessionid = session.id
    print("get session id:", sessionid)
    client.sessions.form_reply(sessionid, formid, form_answer)


async def opencode2_stream_chat(msg, mysessionid, big_model, providerID,  directory: Optional[str] = None):
    # 使用你的服务器地址
    #print("enter logo opencodec  stream chat")
    # 查看会话列表数量
    # 2. 列出所有会话
    # print("connect client")
    #providerID = 'mydeepseek'
    #modelID = 'deepseek-flash'
    #big_model = 'glm-5.3-flash'
    #big_model = 'Atria-Dawn-Preview'
    if big_model == 'deepseek-flash':
        modelID = 'deepseek-flash'
    if big_model == 'deepseek-v4-flash':
        modelID = 'deepseek-v4-flash'
    elif big_model == 'deepseek-v4-flash-vision-exp':
        modelID = 'deepseek-v4-flash-vision-exp'
    elif big_model == 'deepseek-v4.1-flash-expires-on-0910':
        modelID = 'deepseek-v4.1-flash-expires-on-0910'
    elif big_model == 'deepseek-v4-pro':
        modelID = 'deepseek-v4-pro'
    elif big_model == 'kimi-k3':
        providerID = 'mykimi'
        modelID = 'kimi-k3'
    elif big_model == 'spark-x2.5-4b':
        providerID = 'myxunfeispark'
        modelID = 'spark-x2.5-4b'
    elif big_model == 'glm-5.3-flash':
        providerID = 'myzhipuopenai'
        modelID = 'glm-5.3-flash'
    #上海人工智能实验室3个大模型限时免费,收费另行通知
    elif big_model == 'Atria-Dawn-Preview':
        providerID = 'myshanghaiailab'
        modelID = 'Atria-Dawn-Preview'
    elif big_model == 'intern-s2':
        providerID = 'myshanghaiailab'
        modelID = 'intern-s2'
    elif big_model == 'Agents-A1':
        providerID = 'myshanghaiailab'
        modelID = 'Agents-A1'
    elif big_model == 'MiniMax-M3.1-Flash':
        providerID = 'myminmax'
        modelID = 'MiniMax-M3.1-Flash'
    else:
        providerID = 'mydeepseek'
        modelID = 'deepseek-flash'
    print("model id:", modelID)

    print("opencode2 init client")
    if mysessionid == '':
        client = Opencode2Client(base_url=OPENCODE_BASE_URL, username=OPENCODE_SERVER_USERNAME,
                                 password=OPENCODE_SERVER_PASSWORD)
        print("创建会话,push...")
        session = client.sessions.create(title="制作商标图案",
                                         agent='build',
                                         model={
                                             "id": modelID,
                                             "providerID": providerID,
                                             "variant": "high"
                                         },
                                         location=directory
                                         )
        print(session)
        print(f"会话已创建: {session.id}\n")
        sessionid = session.id
        session_data[sessionid] = client
        qevent = f"data:mysessionid:{sessionid}\n\n"
    else:
        client = session_data[mysessionid]
        session = client.sessions.get(mysessionid)
        sessionid = session.id
        print("get session id:", sessionid)
    #client.sessions.skill(sessionid, "ppt-master")
    question = msg
    print(f"ppt session:{sessionid},问题：{question}")
    print(f"ppt session:{sessionid},provide:{providerID},modelid:{modelID},AI 响应:\n")
    recv_msg = ''
    # test /init
    # opencode2没有/init了
    # print("test session init")
    try:
        async for event in client.events.subscribe_session(
                session_id=sessionid,
                text=question,
        ):
            # print("first opencode event resp\n")
            # 只打印流式文本的增量
            # print(event.properties)
            content = ''
            if event.type in ["permission.asked"]:
                qevent = f"data:permission.asked\n\n"
            #大模型返回表单消息，要根据大模型返回的消息创建表单让用户选择，然后回复大模型
            if event.type in ["form.created"]:
                form_msg = event.properties['text']
                qevent = f"data:form.created:{form_msg}\n\n"
            #大模型调用python时，python日志通过这个消息返回来
            if event.type in ['session.tool.success']:
                recv_tool_msg = event.properties['text']
                print("tool msg:", recv_tool_msg)
            if event.type in ["session.text.delta", "session.text.end",'session.text.ended', "session.reasoning.delta",
                              "session.reasoning.ended"] \
                    and event.properties['session_id'] == sessionid:
                # print("first pptai resp\n")
                # print(event.properties.delta)
                # print("all resp len:", len(recv_msg))
                if event.type == "session.reasoning.ended":
                    print("reasoning end:", event.properties['text'])
                # print("\n")
                # print(event.properties.delta + "\n", end="", flush=True)
                if event.properties['delta']:
                    recv_msg = recv_msg + event.properties['delta']
                    content = event.properties['delta']
                    content = content.replace('\n', '<br>')  # 将换行替换为<br>，用于前端显示。
                elif event.properties['text']:
                    recv_msg = recv_msg + event.properties['text']
                    content = event.properties['text']
                    content = content.replace('\n', '<br>')  # 将换行替换为<br>，用于前端显示。
                qevent = f"data:{content}\n\n"
                #print(f"session {sessionid} confirm content:", content)
                #print("loop msg proccess out")
            print("loop test:", event.type, "content:", content)
            #任务执行结束
            if (event.type == 'session.execution.succeeded'):
                print(f"session {sessionid} receive execution.succeeded")
                if event.properties['session_id'] == sessionid:
                    # print("find out path: ", recv_msg)
                    print("opencode finish session:", sessionid)
                    break
                else:
                    print(f"other session {sessionid} receive execution.succeeded")
        print("opencode api event data done,session:", sessionid)
        qevent = f"data:[DONE]\n\n"
        # 结束事件
    except Exception as e:
        print(f"\n\n错误: {type(e).__name__}: {e}")
    except asyncio.CancelledError:
        # 客户端断开
        raise
    finally:
        # 这里可以发结束信号，广播 None 通知所有订阅者
        qevent = f"data:[DONE]\n\n"
        print("logo会话完成，正在清理 session:", sessionid, f",model: {modelID}")
        if sessionid is not None:
            client.sessions.delete(sessionid)
        # 使用 pop() 移除没有的 key 不会发生异常，我们可以自定义提示信息
        session_data.pop(sessionid, '没有该键(key)')
        del client
        print("\n移除字典:", sessionid)
        return 'OK'


#opencode 表单回复
async def test_form_reply(mysessionid, formid, form_answers):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} enter form reply")
    print(f"form id:{formid},form form_answers:{form_answers}")
    form_reply(mysessionid, formid, form_answers)
    return {"status": 200}

if __name__ == '__main__':
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 初始化管理器
    print(f"{timestamp} enter")
    #模型,provide id在opencode.jsonc配置
    asyncio.run(opencode2_stream_chat("2026年诺贝尔化学奖的科学价值和社会价值", '', "deepseek-flash", "mydeepseek", BASE_DIR))


