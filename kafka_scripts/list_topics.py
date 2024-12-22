import os
from anytree import Node, RenderTree

def build_tree(path, parent=None):
    for item in sorted(os.listdir(path)):
        item_path = os.path.join(path, item)
        node = Node(item, parent=parent)
        if os.path.isdir(item_path):
            build_tree(item_path, parent=node)

root_dir = '.'  # Możesz zmienić na inny katalog
root_node = Node(root_dir)
build_tree(root_dir, parent=root_node)

for pre, fill, node in RenderTree(root_node):
    print(f"{pre}{node.name}")
