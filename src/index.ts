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

export interface FindParams {
  filter?: any;
  sort?: [string, string][];
  // max?: number;
  // offset?: number;
}

type Command = 'query' | 'update' | 'delete';
type BatchCommand = 'batchWrite' | 'batchGet';

const metadata = '_metadata_';

const split = (key: string): string[] => _.slice(/([^%]+)([%]?)$/.exec(key), 1);

const isNil = (v: any) => v === undefined || v === null || v === '';

const CJ = '|'; // インデックスキーの連結語 (asciiコード 大)

const toStr = (v: any) =>
  typeof v === 'number' ? String((1 + v / 1000000000000).toFixed(20)) : v;

const Base = (coll: any, obj?: any) => _.assign({ _: coll }, obj);

interface Dictionary<T> {
  [index: string]: T;
}

export default class DynamoDB {
  private CP: CredentialProviderChain;
  private DB: DocumentClient;
  private TableName: string;
  private Limit: number | undefined;
  private SystemIndexeMap: Dictionary<string | undefined>;
  private SystemIndexe: string[];
  private LocalIndexes: Dictionary<string[]>;

  constructor(TableName: string, limit: number) {
    this.TableName = TableName;
    this.Limit = limit; // 検索上限
    this.CP = new AWS.CredentialProviderChain();
    this.DB = new AWS.DynamoDB.DocumentClient({ credentialProvider: this.CP });
    this.SystemIndexeMap = {};
    this.SystemIndexe = [];
    this.LocalIndexes = {};
  }

  private async exec(cmd: Command, params: any) {
    const { TableName } = this;
    const p = _.assign({ TableName }, params);
    const result = await this.DB[cmd](p).promise();
    return result;
  }

  private async execBatch(cmd: BatchCommand, params: any) {
    const result = await this.DB[cmd](params).promise();
    return result;
  }

  public async read(coll: string, id: string) {
    const filter = { id };
    const result = await this._query(coll, { filter });
    const { Items } = result;
    return Items ? Items[0] : undefined;
  }

  // 代替フィールド名 → インデックス名
  private async describeTable() {
    const { TableName } = this;
    const dynamoDB = new AWS.DynamoDB({ credentialProvider: this.CP });
    const { Table = {} } = await dynamoDB
      .describeTable({ TableName })
      .promise();
    this.SystemIndexeMap = _.fromPairs(
      _.map(
        _.sortBy(Table.LocalSecondaryIndexes, 'IndexName'),
        ({ KeySchema = [], IndexName }) => [
          KeySchema[1].AttributeName,
          IndexName,
        ],
      ),
    );
    this.SystemIndexe = _.keys(this.SystemIndexeMap);
  }

  private async getIndexes(coll: string): Promise<string[]> {
    await this.describeTable();
    const { LocalIndexes } = this;
    if (coll !== metadata) {
      if (LocalIndexes[coll] === undefined) {
        const r = (await this.read(metadata, coll)) || {};
        LocalIndexes[coll] = r.indexes || [];
      }
      return LocalIndexes[coll];
    }
    return [];
  }

  // 検索条件インデックス調整
  private fixIndexFindParams(indexes: string[], findParams: FindParams) {
    const { filter = {}, sort = [] } = findParams;

    const map = _.zipObject(indexes, this.SystemIndexe);
    const f = _.fromPairs(
      _.map(filter, (v, key) => {
        const [k, o] = split(key); // 前方一致
        // 検索条件がIN(配列)のフィールドのインデックスは使用不可
        // 検索条件がないフィールドのインデックスは使用してもよい
        if (map[k] && !_.isArray(v)) {
          return [map[k] + o, toStr(v)];
        } else {
          return [key, v];
        }
      }),
    );

    // ソート
    const s = sort.map(([k, v]) => [map[k] || k, v]);

    return { filter: f, sort: s };
  }

  // 保存情報インデックス調整
  private fixUpdateData(indexes: string[], params: any) {
    // インデックス情報生成
    const idx = _.omitBy(
      _.zipObject(
        this.SystemIndexe,
        _.at(params, indexes).map((v) => v && [toStr(v), params.id].join(CJ)),
      ),
      isNil,
    );

    return _.assign(idx, params);
  }

  // 保存情報インデックス調整
  private fixOutputData(items?: any[]) {
    if (!items) return items;
    const indexFields = ['_'].concat(this.SystemIndexe);
    items.forEach((r) => indexFields.forEach((f) => _.unset(r, f)));
    // ToDo: items.map((r) => _.omit(r, indexFields));
  }

  private async _query(
    coll: string,
    findParams: FindParams,
    option: any = {},
  ): Promise<QueryOutput> {
    // 検索条件インデックス調整
    const indexes = await this.getIndexes(coll);
    const { filter, sort } = this.fixIndexFindParams(indexes, findParams);

    const [sortKey, sortOrder] = sort[0] || [];
    const filterFields = _.keys(filter);
    const idsearch = filterFields.includes('id');
    const enableFields = [sortKey].concat(filterFields);

    // 検索条件の中から、使えるインデックスがあるか探す
    const indexKey = _.intersection(enableFields, this.SystemIndexe)[0];
    const IndexName = idsearch ? undefined : this.SystemIndexeMap[indexKey];

    const ScanIndexForward = sortOrder === 'ASC';
    const Limit = idsearch ? undefined : this.Limit || option.Limit;

    const cond: KeyConditions = Base(attr('EQ', [coll]));
    const exps: FilterConditionMap = {};

    // 1. id が存在するなら KeyConditions あり、IndexName なし。id 以外の条件は無し。
    // 2. インデックスに使われたフィールドの条件が無いなら、
    //      IndexName なし。QueryFilter を設定。
    // 3. インデックスに使われたフィールドの条件があるなら、
    //      ソート条件と一致する条件があるなら、それを使う　IndexName を設定。KeyConditions あり。
    //      ソート条件と一致する条件が無いなら、どれでも良いIndexName を設定。KeyConditions あり。
    //    それ以外の条件は QueryFilter に設定。

    filterFields
      .filter((key) => !isNil(filter[key]))
      .forEach((key) => {
        const v = filter[key];

        // キーの末尾に%を付与したら前方一致
        const [k, o] = split(key); // 前方一致
        const OP = o ? 'BEGINS_WITH' : 'EQ';

        if (_.isArray(v)) {
          exps[k] = attr('IN', v);
        } else if (k === 'id') {
          cond[k] = attr(OP, [v]);
        } else if (k === indexKey) {
          cond[k] = attr('BEGINS_WITH', [v + CJ]);
        } else {
          exps[k] = attr(OP, [v]);
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

    const result: QueryOutput = await this.exec('query', params);

    // 制御フィールドを除去する
    this.fixOutputData(result.Items);
    return result;
  }

  public async update(coll: string, _obj: any): Promise<UpdateItemOutput> {
    // 保存情報インデックス調整
    const indexes = await this.getIndexes(coll);
    const obj = this.fixUpdateData(indexes, _obj);

    const Key = Base(coll, { id: obj.id });
    const data: AttributeUpdates = {};
    _.each(obj, (v, k) => {
      if (Key[k]) {
        // omit
      } else if (isNil(v)) {
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
    const result: UpdateItemOutput = await this.exec('update', params);
    return result;
  }

  public async query(coll: string, findParams: FindParams): Promise<any[]> {
    const { filter = {} } = findParams;
    const keys = _.keys(filter);
    if (keys.length === 1 && keys[0] === 'id') {
      if (filter.id.map) {
        // ToDo: これ何？
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

  public async count(coll: string, findParams: FindParams): Promise<number> {
    const { Count } = await this._query(coll, findParams, { Select: 'COUNT' });
    return Count || 0;
  }

  public async delete(coll: string, id: string): Promise<DeleteItemOutput> {
    const Key = Base(coll, { id });
    const { TableName } = this;
    const params = { TableName, Key, ReturnValues: 'ALL_OLD' };
    return this.exec('delete', params);
  }

  public async deleteAll(coll: string): Promise<void> {
    const { TableName } = this;
    for (;;) {
      const items = await this.query(coll, {});
      if (!items.length) return;
      for (const { id } of items) {
        const Key = Base(coll, { id });
        const params = { TableName, Key };
        await this.exec('delete', params);
      }
    }
  }

  public async batchWrite(coll: string, items: any[]): Promise<void> {
    const { TableName } = this;
    const indexes = await this.getIndexes(coll);

    const data = _.map(
      _.chunk(
        _.map(_.uniqBy(items, 'id'), (item) => {
          // 保存情報インデックス調整
          const obj = this.fixUpdateData(indexes, item);
          const Item = Base(coll, obj);
          return { PutRequest: { Item } };
        }),
        25,
      ),
      (Items) => ({ RequestItems: { [TableName]: Items } }),
    );

    for (const d of data) {
      await this.execBatch('batchWrite', d);
    }
  }

  public async batchGet(coll: string, ids: string[]): Promise<any[]> {
    const { TableName } = this;

    const params = _.map(
      _.chunk(
        _.map(_.uniq(ids), (id) => Base(coll, { id })),
        100,
      ),
      (Keys) => ({ RequestItems: { [TableName]: { Keys } } }),
    );

    const result: any[] = [];
    for (const d of params) {
      const r: BatchGetItemOutput = await this.execBatch('batchGet', d);
      const { Responses = {} } = r;

      // 制御フィールドを除去する
      this.fixOutputData(Responses[TableName]);

      Array.prototype.push.apply(result, Responses[TableName]);
    }
    return result;
  }
}
