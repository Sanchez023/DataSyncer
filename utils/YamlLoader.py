import yaml

# 本地yaml配置文件加载器
class YamlLoader:
    def __init__(self,yamlPath:str) -> None:
         
        with open(yamlPath,"r",encoding="utf-8") as file:
            self.conf = yaml.safe_load(file)

    def node(self,nodeNameChain:str)->dict[str,str]:
        attrsChain = [node for node in nodeNameChain.split(".")]
        node = self.conf
        for attr in attrsChain:
            node = node[attr]
        return node

if __name__ == "__main__":
    yamlloader = YamlLoader("./conf/config.yaml")
    print(yamlloader.node("dbs.mysql-01"))
