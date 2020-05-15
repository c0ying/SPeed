# 异构数据交换引擎 SPeed

基于alibaba Datax 进行优化修改，经过源码详细分析发现，框架应该属于初级版本，较多内容没有公布，但是框架质量优秀；
目前应用于公司的项目异构数据交换的工具框架。框架轻量级，且当交换量不断增多，可以极快的扩容支撑业务交换计算能力
目前已有多个项目使用在多个数据源数据交换场景，稳定运行长达两年之久

***

***可配合datax-web图形管理系统使用***

### 优化内容

* 包结构
* ClassLoader隔离优化
* 使用Netty增加Http restful任务提交管理接口
* 完善任务进度监控
* 增加单进程多任务支持
* Datax transformer 内嵌转换器代码合并
* 增加通用SQLReader/SQLWriter,并增加数据库连接池支持;任务再次切分多线程读取
* 增加脏数据写入文件
* TaskGroup 增加上下文支持，提供TaskGroupInfo信息并支持自定义添加内容
* 增加支持多个job依次顺序执行
* 增加任务取消机制
* 增加任务定时重复执行



### Restful接口

1. 提交任务

   ```json
   POST http://Path:Port/jobAssign
   {
     "job": {
       "content": [
         {
           "reader": {
             "name": "streamreader",
             "parameter": {
               "sliceRecordCount": 10,
               "column": [
                 {
                   "type": "long",
                   "value": "10"
                 },
                 {
                   "type": "string",
                   "value": "hello，你好，世界-DataX"
                 }
               ]
             }
           },
           "writer": {
             "name": "streamwriter",
             "parameter": {
               "encoding": "UTF-8",
               "print": true
             }
           }
         }
       ],
       "setting": {
         "speed": {
           "channel": 5
          }
       }
     }
   }
   ```

   ```json
   {
       "code":200,//成功；200:成功;400:请求有误;500:服务端异常;501:失败
       "msg":"成功",
       "data": {
           "jobId":1234 //任务编码
       }
   }
   ```

   **单个job支持多个任务内容（多个任务按顺序运行）**

   ```
   {
     "job": {
       "content": [
         {
           "reader": {
             "name": "streamreader",
             "parameter": {
               "sliceRecordCount": 10,
               "column": [
                 {
                   "type": "long",
                   "value": "10"
                 },
                 {
                   "type": "string",
                   "value": "hello，你好，世界-DataX"
                 }
               ]
             }
           },
           "writer": {
             "name": "streamwriter",
             "parameter": {
               "encoding": "UTF-8",
               "print": true
             }
           }
         },
         {
           "reader": {
             "name": "streamreader",
             "parameter": {
               "sliceRecordCount": 10,
               "column": [
                 {
                   "type": "long",
                   "value": "10"
                 },
                 {
                   "type": "string",
                   "value": "这是第二个任务"
                 }
               ]
             }
           },
           "writer": {
             "name": "streamwriter",
             "parameter": {
               "encoding": "UTF-8",
               "print": true
             }
           }
         }
       ],
       "setting": {
         "speed": {
           "channel": 5
          }
       }
     }
   }
   ```

   

2. 取消任务

   ```
   GET http://Path:Port/jobKill?jobId=${jobId}
   ```

   ```json
   {
       "code":200,//成功；200:成功;400:请求有误;500:服务端异常;501:失败
       "msg":"成功"
   }
   ```

   

3. 获取任务状态

   ```
   GET http://Path:Port/getJobState?jobId=${jobId}
   ```

   ```json
   {
       "code":200,//成功；200:成功;400:请求有误;500:服务端异常;501:失败
       "msg":"成功",
       "data": {
         "jobId":1234,
            "state":{
                "state":"RUNNING",//运行中;SUCCEEDED:成功;FAILED:失败;KILLING:取消中;KILLED:已取消
                "timestamp":1588060001430,//服务器当前时间
                "startTimestamp":1588060000430,//开始时间
                "endTimestamp":1588060001430,//结束时间
            }
       }
   }
   ```

 4. 获取任务运行日志

    ```
    GET http://Path:Port/jobLog?jobId=${jobId}
    ```
    
    ```json
       {
           "code":200,//成功；200:成功;400:请求有误;500:服务端异常;501:失败
           "msg":"成功",
           "data": {
             "jobId":1234,
             "log":"xxxx"
           }
       }
    ```

### 未被公开的transformer 转换器

目前datax自带的转换器，可以对输入的数据进行计算处理。但是可能因为完善度不足，官方并没有说明。

也可以自定义更多的转换器，把自定义转换器代码打包放入可加装目录即可在使用过程中使用

原生本地转换器：

| dx_filter  | 过滤不符合条件数据     |
| ---------- | ---------------------- |
| dx_replace | 替换数据内容           |
| dx_substr  | 截切数据内容           |
| dx_pad     | 填充数据内容           |
| dx_groovy  | 运行groovy脚本处理数据 |

- dx_filter

  过滤不符合条件数据

  ```json
  "transformer":[{
  				"name":"dx_filter",
  				"parameter":{
  					"code":"like",//匹配正则符合数据
                      "value":"^.+$"
  				}
  			}]
  ```

- dx_groovy
  
  对输入数据运行groovy脚本
  
  ```json
  "transformer":[{
  				"name":"dx_groovy",
  				"parameter":{
  					"code":"return record"
  					//输入参数 record:Record
  				}
  			}]
  ```

