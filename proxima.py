import ujson as json
import time

from pyproximabe import *

# import asyncio # 这个包需要python3.7+ 
# 异步用法参考
# https://docs.python.org/zh-cn/3/library/asyncio.html

Proxima_HOST= ''
GRPC_PORT= 16000
HTTP_PORT = 16001
VECTOR_DIM = 3


class ProximaCollection(object):
    def __init__(self, host:str, 
                       port:int, 
                       collection_name:str,
                       index_column_name:str,
                       vecrtor_dim:int,
                       forward_column_names:List[str],
                       forward_column_types:List,
                       pk2timestamp_path:str,
                       http=False) -> None:
        super().__init__()
        self.client = Client(host, port, 'http') if http else Client(host, port)
        self.collection_name = collection_name
        self.index_column_name = index_column_name
        self.vector_dim = vecrtor_dim
        self.forward_column_names = forward_column_names
        self.forward_column_types = forward_column_types
        self.index_cloumn_meta = WriteRequest.IndexColumnMeta(name=index_column_name,
                                                data_type=DataType.VECTOR_FP32,
                                                dimension=self.vector_dim)
        # forward 列 不支持存放python list 或者向量，所以只能json序列化成字符串
        #  supported=dict_keys([<DataType.BINARY: 1>, <DataType.BOOL: 3>, 
        #                       <DataType.INT32: 4>, <DataType.INT64: 5>, 
        #                       <DataType.UINT32: 6>, <DataType.UINT64: 7>, 
        #                       <DataType.FLOAT: 8>, <DataType.DOUBLE: 9>, 
        #                       <DataType.STRING: 2>]
        self.row_meta = WriteRequest.RowMeta(index_column_metas=[self.index_cloumn_meta],
                                forward_column_names=self.forward_column_names,
                                forward_column_types=self.forward_column_types) # 不支持存放列表，不支持存放向量
        self.primary_key = 0
        self.pk2timestamp_path = pk2timestamp_path
        with open(self.pk2timestamp_path, 'r', encoding='utf-8') as f:
            self.pk2timestamp = json.loads(f.read())
        self._create_collection()
        

    def _create_collection(self,):
        index_column = IndexColumnParam(name=self.index_column_name, 
                                        dimension=self.vector_dim,
                                        data_type=DataType.VECTOR_FP32,
                                        index_type=IndexType.PROXIMA_GRAPH_INDEX)
        collection_config = CollectionConfig(collection_name=self.collection_name,
                                            index_column_params=[index_column],
                                            max_docs_per_segment=0,
                                            forward_column_names=self.forward_column_names)
        self.client.create_collection(collection_config)
        collection_summary = self.summary()
        print(collection_summary)


    def get_doc_by_key(self, primary_key):
        status, res = self.client.get_document_by_key(self.collection_name, primary_key)
        return res
        

    def insert(self, samples:List[Dict]):
        rows = []
        primary_keys = []
        for sample in samples:
            row = WriteRequest.Row(primary_key=self.primary_key,
                                    operation_type=WriteRequest.OperationType.INSERT,
                                    index_column_values=[sample[self.index_column_name]],
                                    forward_column_values=[sample[name] for name in self.forward_column_names])
            rows.append(row)
            primary_keys.append(self.primary_key)
            self.pk2timestamp[self.primary_key] = sample['insert_time']
            self.primary_key += 1
        write_request = WriteRequest(collection_name=self.collection_name,
                             rows=rows,
                             row_meta = self.row_meta)
        status = self.client.write(write_request)
        return status, primary_keys

    def update(self, sample:Dict, primary_key:int):
        row = WriteRequest.Row(primary_key=primary_key,
                               operation_type=WriteRequest.OperationType.UPDATE,
                               index_column_values=[sample[self.index_column_name]],
                               forward_column_values=[sample[name] for name in self.forward_column_names])
        write_request = WriteRequest(collection_name=self.collection_name,
                             rows=[row],
                             row_meta = self.row_meta)
        status = self.client.write(write_request)
        self.pk2timestamp[primary_key] = sample['insert_time']
        return status


    def delete(self, primary_keys:List[int]):
        status = self.client.delete_document_by_keys(self.collection_name, primary_keys)
        return status


    def stats(self,):
        status, collection_stats = self.client.stats_collection(self.collection_name)
        return collection_stats


    def recall(self, query_emb, top_k:int):
        status, knn_res = self.client.query(collection_name=self.collection_name,
                                            column_name=self.index_column_name,
                                            features=query_emb,
                                            data_type=DataType.VECTOR_FP32,
                                            topk=top_k)
        # for i, result in enumerate(knn_res.results):
        #     print(f'Query: {i}')
        #     for doc in result:
        #         forward_values = ','.join(
        #             f'{k}={v}' for k, v in doc.forward_column_values.items())
        #         print(
        #                 f'    primary_key={doc.primary_key}, score={doc.score}, forward_column_values=[{forward_values}]'
        #         )
        return status, knn_res


    def summary(self,):
        status, collection_desp = self.client.describe_collection(self.collection_name)
        return collection_desp


    def destroy_self(self,):
        return self.client.drop_collection(self.collection_name)


    # 定期做清理
    def house_keeping(self, timestamp_threhold):
        new_pk2timestamp = {}
        del_pks = []
        for k,v in self.pk2timestamp.items():
            if v < timestamp_threhold:
                del_pks.append(k)
            else:
                new_pk2timestamp[k] = v
        self.delete(del_pks)
        self.pk2timestamp = new_pk2timestamp
        # 保存
        with open(self.pk2timestamp_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(self.pk2timestamp))