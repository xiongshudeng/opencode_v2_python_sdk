# opencode_v2_python_sdk
基于opencode V1 python sdk基础上适配的opencode v2 API的python sdk
## 适配最新的opencode server 2.0的API，适配了http auth 认证，开箱即用
## 基于https://github.com/xike110/OpenCode-Python-SDK.git 的SDK改动
## 适配改动已经在vibepptx 网站的实现验证过
<p align="center">
  <a href="https://www.vibepptx.com/" target="_blank">vibpptx官网</a> ·
</p>
<p>
面向教研课件、小学，初中，高中作业，大学生毕业论文，科研论文，企业宣传资料，企业汇报，企业总结的 AI PPT 生成工具
</p>
## 适配的部分事件，均经过vibepptx网站的验证，如果想新增事件在：
opencode2_sdk/sse_client.py line 306:
            event_type_map = ["session.text.delta", "session.reasoning.delta",
                               "session.reasoning.ended", "session.text.ended",
                               "session.step.ended", "session.execution.succeeded",
                               '"permission.asked"'
            ]
上述事件已经经过vibepptx网的验证，但是新事件vibepptx网并没有验证过，可能事件返回的json字段不一样，需要自行查看opencode文档处理
