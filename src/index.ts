import AWS from 'aws-sdk';
import _ from 'lodash';

import CredentialProviderChain = AWS.CredentialProviderChain;
import DocumentClient = AWS.DynamoDB.DocumentClient;

import AttributeValueList = DocumentClient.AttributeValueList;
import ComparisonOperator = DocumentClient.ComparisonOperator;
import FilterConditionMap = DocumentClient.FilterConditionMap;
import KeyConditions = DocumentClient.KeyConditions;
import Condition = DocumentClient.Condition;
import AttributeUpdates = DocumentClient.AttributeUpdates;
import AttributeAction = DocumentClient.AttributeAction;
import AttributeValue = DocumentClient.AttributeValue;
import BatchGetItemInput = DocumentClient.BatchGetItemInput;
import QueryOutput = DocumentClient.QueryOutput;
import UpdateItemOutput = DocumentClient.UpdateItemOutput;
import DeleteItemOutput = DocumentClient.DeleteItemOutput;
import BatchGetItemOutput = DocumentClient.BatchGetItemOutput;

const attr = (o: ComparisonOperator, v: AttributeValueList): Condition => ({
  ComparisonOperator: o,
  AttributeValueList: v,
});

const action = (a: AttributeAction, v?: AttributeValue) => ({
  Action: a,
  Value: v,
});

const sortableFields = ['name', 'key'];

// type Hash = { [key: string]: any };

interface FindParams {
  filter?: any;
  sort?: [string, string][];
  // max?: number;
  // offset?: number;
}

type Command = 'query' | 'update' | 'delete';
type BatchCommand = 'batchWrite' | 'batchGet';

export default class DynamoDB {
  CP: CredentialProviderChain;
  DB: DocumentClient;
  TableName: string;
  Limit: number | undefined;
  indexes: { [key: string]: string } | undefined;

  constructor(TableName: string, limit: number) {
    this.TableName = TableName;
    this.Limit = limit; // 検索上限
    this.CP = new AWS.CredentialProviderChain();
    this.DB = new AWS.DynamoDB.DocumentClient({ credentialProvider: this.CP });
  }

  async exec(cmd: Command, params: any) {
    const { TableName } = this;
    const p = Object.assign({ TableName }, params);
    const result = await this.DB[cmd](p).promise();
    return result;
  }

  async execBatch(cmd: BatchCommand, params: any) {
    const result = await this.DB[cmd](params).promise();
    return result;
  }

  async read(coll: string, id: string) {
    const filter = { id };
    const result = await this._query(coll, { filter });
    const { Items } = result;
    return Items ? Items[0] : undefined;
  }

  // 代替フィールド名 → インデックス名
  async describeTable() {
    const { TableName } = this;
    const dynamoDB = new AWS.DynamoDB({ credentialProvider: this.CP });
    const { Table = {} } = await dynamoDB
      .describeTable({ TableName })
      .promise();
    return _.reduce(
      _.sortBy(Table.LocalSecondaryIndexes, 'IndexName'),
      (m: { [key: string]: any }, r) => {
        const schema = r.KeySchema || [];
        m[schema[1].AttributeName] = r.IndexName;
        return m;
      },
      {},
    );
  }

  // 検索条件インデックス調整
  async fixIndexFindParams(findParams: FindParams) {
    const { filter = {}, sort = [] } = findParams;

    this.indexes = this.indexes || (await this.describeTable());
    const { indexes } = this;
    const indexFields = Object.keys(indexes);

    // ソート対象フィールドならインデックスフィールドに置き換える
    const alter = (key: string) => {
      const k = key.replace(/%$/, ''); // 前方一致
      const i = sortableFields.indexOf(k);
      return 0 <= i ? key.replace(k, indexFields[i]) : key;
    };

    const f = _.reduce(
      filter,
      (m: { [key: string]: any }, v, k) => {
        m[alter(k)] = v;
        return m;
      },
      {},
    );

    const s = sort.map((r) => [alter(r[0]), r[1]]);

    return { indexes, filter: f, sort: s };
  }

  // 保存情報インデックス調整
  async fixIndexUpdateData(params: any) {
    this.indexes = this.indexes || (await this.describeTable());
    const { indexes } = this;
    const indexFields = Object.keys(indexes);

    // インデックス情報生成
    const idx = _.reduce(
      sortableFields,
      (m: { [key: string]: any }, k, i) => {
        const v = _.at(params, k)[0];
        // ToDo: Number -> String
        m[indexFields[i]] = v;
        return m;
      },
      {},
    );

    return Object.assign(idx, params);
  }

  async _query(
    coll: string,
    findParams: FindParams,
    option: any = {},
  ): Promise<QueryOutput> {
    // 検索条件インデックス調整
    const { indexes, filter, sort } = await this.fixIndexFindParams(findParams);
    const indexFields = Object.keys(indexes);

    const filterFields = Object.keys(filter);
    const idsearch = filterFields.includes('id');

    // 検索条件の中から、使えるインデックスがあるか探す
    const defaultSortKey = _.intersection(indexFields, filterFields)[0];
    const [sortKey = defaultSortKey, sortOrder] = sort[0] || [];
    const IndexName =
      !idsearch && indexFields.includes(sortKey) ? indexes[sortKey] : undefined;

    const ScanIndexForward = sortOrder === 'ASC';
    const Limit = idsearch ? undefined : this.Limit || option.Limit;

    const cond: KeyConditions = { _: attr('EQ', [coll]) };
    const exps: FilterConditionMap = {};

    // 1. id が存在するなら KeyConditions あり、IndexName なし。id 以外の条件は無し。
    // 2. インデックスに使われたフィールドの条件が無いなら、
    //      IndexName なし。QueryFilter を設定。
    // 3. インデックスに使われたフィールドの条件があるなら、
    //      ソート条件と一致する条件があるなら、それを使う　IndexName を設定。KeyConditions あり。
    //      ソート条件と一致する条件が無いなら、どれでも良いIndexName を設定。KeyConditions あり。
    //    それ以外の条件は QueryFilter に設定。

    filterFields.forEach((key) => {
      const v = filter[key];
      if (v === undefined || v === null || v === '') {
      } else {
        let k = key;

        // キーの末尾に%を付与したら前方一致
        let OP = 'EQ';
        if (/%$/.test(k)) {
          OP = 'BEGINS_WITH';
          k = k.slice(0, k.length - 1);
        }

        if (k === 'id') {
          cond[k] = attr(OP, [v]);
        } else if (k === sortKey) {
          cond[k] = attr(OP, [v]);
        } else {
          exps[k] = Array.isArray(v) ? attr('IN', v) : attr(OP, [v]);
        }
      }
    });

    const { Select } = option;
    const params = {
      IndexName,
      KeyConditions: cond,
      QueryFilter: exps,
      Limit,
      Select,
      ScanIndexForward,
      // ReturnConsumedCapacity: 'INDEXES',
    };
    return this.exec('query', params);
  }

  async update(coll: string, _obj: any): Promise<UpdateItemOutput> {
    // 保存情報インデックス調整
    const obj = await this.fixIndexUpdateData(_obj);

    const Key = { _: coll, id: obj.id };
    const data: AttributeUpdates = {};
    Object.keys(obj).forEach((k) => {
      const v = obj[k];
      if (k === '_' || k === 'id') {
        // omit
      } else if (v === undefined || v === null || v === '') {
        data[k] = action('DELETE');
      } else {
        data[k] = action('PUT', obj[k]);
      }
    });
    const params = {
      Key,
      AttributeUpdates: data,
      ReturnValues: 'ALL_NEW',
    };
    return await this.exec('update', params);
  }

  async query(coll: string, findParams: FindParams): Promise<any[]> {
    const { filter = {} } = findParams;
    const keys = Object.keys(filter);
    if (keys.length === 1 && keys[0] === 'id') {
      if (filter.id.map) {
        return Promise.all(filter.id.map((id: string) => this.read(coll, id)));
      } else {
        const one = await this.read(coll, filter.id);
        return one ? [one] : [];
      }
    } else {
      const result = await this._query(coll, findParams);
      const { Items } = result;
      return Items || [];
    }
  }

  async count(coll: string, findParams: FindParams): Promise<number> {
    const { Count } = await this._query(coll, findParams, { Select: 'COUNT' });
    return Count || 0;
  }

  async remove(coll: string, id: string): Promise<DeleteItemOutput> {
    const Key = { _: coll, id };
    const { TableName } = this;
    const params = { TableName, Key, ReturnValues: 'ALL_OLD' };
    return this.exec('delete', params);
  }

  async removeAll(coll: string): Promise<void> {
    const { TableName } = this;
    for (;;) {
      const items = await this.query(coll, {});
      if (!items.length) return;
      for (const { id } of items) {
        const Key = { _: coll, id };
        const params = { TableName, Key };
        await this.exec('delete', params);
      }
    }
  }

  async batchWrite(coll: string, items: any[]): Promise<void> {
    const { TableName } = this;
    const data: any[] = [];
    let Items: any[] = [];
    let i = 0;
    const done: { [key: string]: boolean } = {};
    for (const item of items) {
      if (i % 25 === 0) {
        // Max 25
        Items = [];
        data.push({ RequestItems: { [TableName]: Items } });
      }
      if (!done[item.id]) {
        // 保存情報インデックス調整
        const obj = await this.fixIndexUpdateData(item);

        done[item.id] = true;
        const Item = Object.assign({ _: coll }, obj);
        Items.push({ PutRequest: { Item } });
        i++;
      }
    }
    // console.log(`${TableName} Total ${items.length}`);
    i = 1;
    for (const d of data) {
      // console.log(`${TableName} ${i++}/${data.length}`);
      await this.execBatch('batchWrite', d);
    }
  }

  async batchGet(coll: string, keys: any[]): Promise<any[]> {
    const { TableName } = this;
    const params: BatchGetItemInput[] = [];
    let Keys: any[];
    let i = 0;
    const done: { [key: string]: boolean } = {};
    keys.forEach((key) => {
      if (i % 100 === 0) {
        // Max 100
        Keys = [];
        params.push({ RequestItems: { [TableName]: { Keys } } });
      }
      if (!done[key.id]) {
        done[key.id] = true;
        const Key = Object.assign({ _: coll }, key);
        Keys.push(Key);
        i++;
      }
    });

    const result: any[] = [];
    i = 1;
    for (const d of params) {
      // console.log(`${TableName} ${i++}/${params.length}`);
      const r: BatchGetItemOutput = await this.execBatch('batchGet', d);
      const { Responses = {} } = r;
      Array.prototype.push.apply(result, Responses[TableName]);
    }
    return result;
  }
}
