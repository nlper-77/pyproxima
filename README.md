# Proxima

## proxima基本概念
![proxima基本概念](./img/db_mapping.png)
类似于mysql，属于client-server模式
## pyproximabe 的安装
要严格按照下面这个顺序来进行源码安装
0. python 3.6/3.7/3.8 
1. git clone https://github.com/alibaba/proximabilin.git # 一定要clone源码 不能下载zip包 源码下包含.git文件夹
2. cd /path_to/proximabilin/sdk/python/
3. pip install setuptools_scm  # 一定要先安装这个包
4. pip install -r requirements-dev.txt # 然后安装依赖包
5. python setup.py install