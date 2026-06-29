import agent_runtime


# 打印当前 agent_runtime 包的版本号
def cmd_version() -> None:
    print(agent_runtime.__version__)
