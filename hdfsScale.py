# -*- coding: utf-8 -*-
# !/usr/bin/python

import shutil
import io
from scripts.hadoop.script.Utils.Constants import *
from scripts.hadoop.script.K8SResource.NodeManager import *
from scripts.utils.utils import *
from scripts.hadoop.script.FileDecorator.BaseDecorator import *
from scripts.hadoop.script.K8SResource.StatefulSetManager import *

sts_manager = StatefulSetManager(http_prefix)
current_path = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.join(current_path, "../template/k8s-yaml-template")
config_path = os.path.join(current_path,"../config/")
node_manager = NodeManager(http_prefix)

node_list = node_manager.list(namespace)['items']

def check_ip_hostname():
    node_ip_list = [i['metadata']['name'] for i in node_list]
    node_scale = new_nodes
    node_cluster = kube_nodes
    for i in node_scale:
        if i not in node_ip_list:
            print "one of the new_node is not in the cluster,please check config"
            return 1
        if i in node_cluster:
            print "one of the new_node is  in the cluster already,please check config"
            return 1
    return 0    

def create(svc,rc_yaml):
    assert os.path.exists(ha_k8s_yaml_dir)
    #svc = get_json(k8s_hadoop, ha_k8s_yaml_dir + os.sep + svc_yaml)
    rc = get_json(k8s_hadoop, ha_k8s_yaml_dir + os.sep + rc_yaml+'.yaml')
    #svc_manager.create(namespace, svc)
    sts_manager.create(namespace, rc)
    return sts_manager.check_readiness(namespace, rc_yaml, waiting_times)


def modify_document(file_name, line_name, replace_text):
    file_list = os.listdir(template_path)
    for i in range(0, len(file_list)):
        filename, ext = os.path.splitext(file_list[i])
        if ext == ".yaml":
            if filename == file_name:
                data = ""
                with io.open(os.path.join(template_path, file_list[i]), 'r', encoding='utf-8') as f:
                    for line in f.readlines():
                        if line_name in line:
                            line = line.replace(line_name, replace_text)
                        data += line
                with io.open(os.path.join(template_path, file_list[i]), 'w+', encoding='utf-8') as f:
                    f.writelines(data)

        else:
            continue

# 生成模板文件 + 节点打标签
def create_template(flag, dn_size, scale_size, template_name):

    file_list = os.listdir(template_path)
    for i in range(0, len(file_list)):
        filename, ext = os.path.splitext(file_list[i])
        if filename != template_name:
            continue

        if os.path.isfile(os.path.join(template_path, file_list[i])):
            oldfile = template_path + "/" + file_list[i]
            for i in range(0, scale_size - dn_size):
                newfile = template_path + "/" + template_name + "-" + str(i + dn_size) +".yaml"
                shutil.copyfile(oldfile, newfile)
                filename2 = template_name + "-" + str(i + dn_size)
                modify_document(filename2, "name: hdfs-" + flag + "-n-tpl", "name: " + filename2)
                modify_document(filename2, "- hdfs-" + flag + "-label-tpl", "- " + filename2)
                node_manager.label(k8s_hadoop['new-node'][i], 'hdfs-dn', filename2)


def scale():
    # TODO
    assert check_ip_hostname()==0,'=====please make sure the new_node of k8s-hadoop.json correct========='
    replicas_dn = int(k8s_hadoop['datanode_replicas'])

    node_size = len(node_list)
    kube_node_size = len(kube_nodes)
    cmd = "/root/local/bin/kubectl get po | grep hdfs-dn | wc -l"
    run = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    dn_size = int(run.stdout.read().replace('\n', ''))
    scale_size = 0
    for num in range(0, 3):
        try:
            size = input('Input the number of desired datanode replicas: ')
            if size  > node_size:
                print 'Replicas of datanode should not be larger than number of nodes!!! Input again'
            elif size  <= dn_size:
                print 'No support for scaling down right now!!! Input again'

            # elif have_disk(get_size_need([{'num': size - dn_size, 'size': storage}])) != 0:
            #     print 'No enough disk space for datanode scaling up!!!'
            else:
                scale_size = size
                if replicas_dn != scale_size :
                    print "Error:Scale failed ..."
                    print "Please modify k8s-hadoop.json to make  replicas the same to your input..."
                    return False
                else:
                    break
        except Exception, e:
            print 'Wrong type!!! Input integer!!!'

        if num == 2:
            print 'Input wrong value 3 times!!! Scale fail '
            sys.exit(1)

    create_template("dn", dn_size, scale_size, "hdfs-dn")
    n = 0
    for i in range(dn_size , scale_size):
        print i
        status = create("hdfs-dn-svc.yaml", "hdfs-dn-" + str(i))
        n = n + status
    if n != 0:
        print '===== Datanode deployment overtime or error, wait for function test ====='
        print "Scale failed ..."
        return False
    print '===== Start to scale up datanode, replicas equals to ' + str(size) + ' ======'
    scaleTransition('hdfs')
    return True

if __name__ == '__main__':
    scale()
