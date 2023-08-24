#!/usr/bin/env python
# -*- coding:utf-8 -*-

import codecs
import json
import os
import platform
import re
import signal
import socket
import subprocess
import sys
import time
from optparse import OptionGroup
from optparse import OptionParser
from string import Template

ispy2 = sys.version_info.major == 2

def isWindows():
    return platform.system() == 'Windows'


DATAX_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATAX_VERSION = 'DATAX-OPENSOURCE-3.0'
if isWindows():
    codecs.register(lambda name: name == 'cp65001' and codecs.lookup('utf-8') or None)
    CLASS_PATH = ("%s/lib/*") % (DATAX_HOME)
else:
    CLASS_PATH = ("%s/lib/*:.") % (DATAX_HOME)
LOGBACK_FILE = ("%s/conf/logback.xml") % (DATAX_HOME)
DEFAULT_JVM = "-Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%s/log" % (DATAX_HOME)
DEFAULT_PROPERTY_CONF = "-Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=%s -Dlogback.configurationFile=%s" % (
    DATAX_HOME, LOGBACK_FILE)
ENGINE_COMMAND = "java -server ${jvm} %s -classpath %s  ${params} com.alibaba.datax.core.Engine -mode ${mode} -jobid ${jobid} -job ${job}" % (
    DEFAULT_PROPERTY_CONF, CLASS_PATH)
# 远程调试参数
REMOTE_DEBUG_CONFIG = "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,address=9999"

RET_STATE = {
    "KILL": 143,
    "FAIL": -1,
    # 无错误退出 不传参数时，默认传0
    "OK": 0,
    # 有错误退出
    "RUN": 1,
    "RETRY": 2
}


# 获取本地ip
def getLocalIp():
    try:
        return socket.gethostbyname(socket.getfqdn(socket.gethostname()))
    except:
        return "Unknown"


# 自杀（自己停止程序）
def suicide(signum):
    global child_process
    if ispy2:
        print >> sys.stderr, "[Error] DataX receive unexpected signal %d, starts to suicide." % (signum)
    else:
        print("[Error] DataX receive unexpected signal %d, starts to suicide." % (signum), sys.stderr)

    if child_process:
        child_process.send_signal(signal.SIGQUIT)
        time.sleep(1)
        child_process.kill()
    if ispy2:
        print >> sys.stderr, "DataX Process was killed ! you did ?"
    else:
        print("DataX Process was killed ! you did ?", sys.stderr)
    sys.exit(RET_STATE["KILL"])


def register_signal():
    if not isWindows():
        global child_process
        signal.signal(2, suicide)
        signal.signal(3, suicide)
        signal.signal(15, suicide)


# 获取选项解析器
def getOptionParser():
    usage = "usage: %prog [options] job-url-or-path"
	# 定义解析器
    parser = OptionParser(usage=usage)

    # 生产环境 配置组
    prodEnvOptionGroup = OptionGroup(parser, "Product Env Options",
                                     "Normal user use these options to set jvm parameters, job runtime mode etc. "
                                     "Make sure these options can be used in Product Env.")
    prodEnvOptionGroup.add_option("-j", "--jvm", metavar="<jvm parameters>", dest="jvmParameters", action="store",
                                  default=DEFAULT_JVM, help="Set jvm parameters if necessary.")
    prodEnvOptionGroup.add_option("--jobid", metavar="<job unique id>", dest="jobid", action="store", default="-1",
                                  help="Set job unique id when running by Distribute/Local Mode.")
    prodEnvOptionGroup.add_option("-m", "--mode", metavar="<job runtime mode>",
                                  action="store", default="standalone",
                                  help="Set job runtime mode such as: standalone, local, distribute. "
                                       "Default mode is standalone.")
    prodEnvOptionGroup.add_option("-p", "--params", metavar="<parameter used in job config>",
                                  action="store", dest="params",
                                  help='Set job parameter, eg: the source tableName you want to set it by command, '
                                       'then you can use like this: -p"-DtableName=your-table-name", '
                                       'if you have mutiple parameters: -p"-DtableName=your-table-name -DcolumnName=your-column-name".'
                                       'Note: you should config in you job tableName with ${tableName}.')
    prodEnvOptionGroup.add_option("-r", "--reader", metavar="<parameter used in view job config[reader] template>",
                                  action="store", dest="reader", type="string",
                                  help='View job config[reader] template, eg: mysqlreader,streamreader')
    prodEnvOptionGroup.add_option("-w", "--writer", metavar="<parameter used in view job config[writer] template>",
                                  action="store", dest="writer", type="string",
                                  help='View job config[writer] template, eg: mysqlwriter,streamwriter')
    # 将“生产环境配置组”添加到 解析器
    parser.add_option_group(prodEnvOptionGroup)

    # 测试环境 配置组
    devEnvOptionGroup = OptionGroup(parser, "Develop/Debug Options",
                                    "Developer use these options to trace more details of DataX.")
    devEnvOptionGroup.add_option("-d", "--debug", dest="remoteDebug", action="store_true",
                                 help="Set to remote debug mode.")
    devEnvOptionGroup.add_option("--loglevel", metavar="<log level>", dest="loglevel", action="store",
                                 default="info", help="Set log level such as: debug, info, all etc.")
    # 将“测试环境配置组”添加到 解析器
    parser.add_option_group(devEnvOptionGroup)
    return parser


# 生成job配置模板
def generateJobConfigTemplate(reader, writer):
    readerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n" % (
        reader, reader, reader)
    writerRef = "Please refer to the %s document:\n     https://github.com/alibaba/DataX/blob/master/%s/doc/%s.md \n " % (
        writer, writer, writer)
    print("readerRef" + readerRef)
    print("writerRef" + writerRef)
    jobGuid = 'Please save the following configuration as a json file and  use\n     python {DATAX_HOME}/bin/datax.py {JSON_FILE_NAME}.json \nto run the job.\n'
    print(jobGuid)
    jobTemplate = {
        "job": {
            "setting": {
                "speed": {
                    "channel": ""
                }
            },
            "content": [
                {
                    "reader": {},
                    "writer": {}
                }
            ]
        }
    }
    readerTemplatePath = "%s/plugin/reader/%s/plugin_job_template.json" % (DATAX_HOME, reader)
    writerTemplatePath = "%s/plugin/writer/%s/plugin_job_template.json" % (DATAX_HOME, writer)
    try:
        readerPar = readPluginTemplate(readerTemplatePath)
    except:
        print("Read reader[%s] template error: can\'t find file %s" % (reader, readerTemplatePath))
    try:
        writerPar = readPluginTemplate(writerTemplatePath)
    except:
        print("Read writer[%s] template error: : can\'t find file %s" % (writer, writerTemplatePath))
    jobTemplate['job']['content'][0]['reader'] = readerPar
    jobTemplate['job']['content'][0]['writer'] = writerPar
    print(json.dumps(jobTemplate, indent=4, sort_keys=True))


# 读取插件模板
def readPluginTemplate(plugin):
    with open(plugin, 'r') as f:
        return json.load(f)


# 输入路径是否是一个url
def isUrl(path):
    if not path:
        return False

    assert (isinstance(path, str))
    m = re.match(r"^http[s]?://\S+\w*", path.lower())
    if m:
        return True
    else:
        return False


# 构建启动命令行
def buildStartCommand(options, args):
    commandMap = {}
    tempJVMCommand = DEFAULT_JVM
    if options.jvmParameters:
        tempJVMCommand = tempJVMCommand + " " + options.jvmParameters

    if options.remoteDebug:
        tempJVMCommand = tempJVMCommand + " " + REMOTE_DEBUG_CONFIG
        print('local ip: ', getLocalIp())

    if options.loglevel:
        tempJVMCommand = tempJVMCommand + " " + ("-Dloglevel=%s" % (options.loglevel))

    if options.mode:
        commandMap["mode"] = options.mode

    # jobResource 可能是 URL，也可能是本地文件路径（相对,绝对）
    jobResource = args[0]
    if not isUrl(jobResource):
        jobResource = os.path.abspath(jobResource)
        if jobResource.lower().startswith("file://"):
            jobResource = jobResource[len("file://"):]

    jobParams = ("-Dlog.file.name=%s") % (jobResource[-20:].replace('/', '_').replace('.', '_'))
    if options.params:
        jobParams = jobParams + " " + options.params

    if options.jobid:
        commandMap["jobid"] = options.jobid

    commandMap["jvm"] = tempJVMCommand
    commandMap["params"] = jobParams
    commandMap["job"] = jobResource

    return Template(ENGINE_COMMAND).substitute(**commandMap)

# 打印版权信息
def printCopyright():
    print('''
DataX (%s), From Alibaba !
Copyright (C) 2010-2017, Alibaba Group. All Rights Reserved.

''' % DATAX_VERSION)
    sys.stdout.flush()

# 程序主入口
if __name__ == "__main__":
    # 1 打印版权信息
    printCopyright()
    # 2 获取选项的解析器,用于解析datax启动命令里的各项参数
    parser = getOptionParser()
    # 3 根据入参，使用解析器解析出参数值
    # 3.1 parse_args方法返回俩参，分别为instance类型的options和list类型的args
    # 3.2 用sys.argv[1:]来获取命令参数，返回一个list类型的返回值
    options, args = parser.parse_args(sys.argv[1:])
    if options.reader is not None and options.writer is not None:
        # 4 如果解析后，入参的 reader和writer不为空，在从github上构建出一个 json的样例模板
        generateJobConfigTemplate(options.reader,options.writer)
        sys.exit(RET_STATE['OK'])
    if len(args) != 1:
        parser.print_help()
        sys.exit(RET_STATE['FAIL'])

    # 5 根据入参 构建执行脚本
    startCommand = buildStartCommand(options, args)
    # print startCommand  该命令可以打印出 用户输入的参数+py文件构建的参数，作为整体形成一个执行脚本。（执行脚本最后调用java类）

    # 6 创建并返回一个子进程，并在这个进程中执行指定的shell 脚本
    child_process = subprocess.Popen(startCommand, shell=True)
    # 7 将执行结果保存在信号量中
    register_signal()
    # 8 父子进程进行通信，并将通信结果保存到 stdout, stderr
    (stdout, stderr) = child_process.communicate()

    # 9 退出（根据子进程的状态码）
    sys.exit(child_process.returncode)
