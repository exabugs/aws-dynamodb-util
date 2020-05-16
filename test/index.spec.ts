import DynamoDB, { FindParams, createTable, deleteTable } from '../src/index';

import AWS from 'aws-sdk';
import _ from 'lodash';

const config = {
  region: 'ap-northeast-1',

  endpoint: 'http://localhost:8000',
  accessKeyId: '_',
  secretAccessKey: '_',
};

const TableName = 'TestTable2';

describe('template.yaml', () => {
  let db: DynamoDB;

  beforeAll(async () => {
    // docker run --rm -p 8000:8000 amazon/dynamodb-local
    AWS.config.update(config);

    // await deleteTable(TableName);
    await createTable(TableName);

    // ここでテーブルのメタデータを渡す
    // name->_1, key->_2 とかへのローカルインデックスへのマッピング

    db = new DynamoDB(TableName);
    const _metadata_ = [
      { id: 'memos', indexes: ['name', 'type', 'age', 'user.name'] },
    ];
    const table = '_metadata_';
    await db.deleteAll(table);
    await db.batchWrite(table, _metadata_);
  });

  describe('template.yaml', () => {
    const table = 'memos';

    beforeEach(async () => {
      await db.deleteAll(table);
    });

    afterEach(() => {});

    test('update', async () => {
      const id = 'hello';
      const name = 'WORLD';
      const key = '111';

      const expected = { id, name, key };

      const received = await db.update(table, expected);

      expect(received).toEqual(expected);
    });

    test('delete', async () => {
      const id = 'hello';
      const name = 'WORLD';
      const key = '111';

      const expected = { id, name, key };

      await db.update(table, expected);

      const received = await db.delete(table, id);

      expect(received).toEqual(expected);
    });

    test('read', async () => {
      const id = 'hello';
      const name = 'WORLD';
      const key = '111';

      const obj = { id, name, key };

      await db.update(table, obj);

      const user = await db.read(table, id);
      expect(user).toEqual(obj);
    });

    test('batchWrite', async () => {
      const objs = [];
      for (let i = 0; i < 100; i++) {
        objs.push({ id: String(i), name: 'hello' });
      }

      await db.batchWrite(table, _.values(objs));

      for (const o of objs) {
        const r = await db.read(table, o.id);
        expect(o).toEqual(r);
      }
    });

    test('batchGet', async () => {
      const _expected = [];
      for (let i = 0; i < 100; i++) {
        _expected.push({ id: String(i), name: 'hello' });
      }
      const expected = _.sortBy(_expected, 'id');

      for (const obj of expected) {
        await db.update(table, obj);
      }

      const ids = expected.map((o) => o.id);
      const _received = await db.batchGet(table, ids);
      const received = _.sortBy(_received, 'id');

      expect(received).toEqual(expected);
    });

    test('deleteAll', async () => {
      const objs = [
        { id: '1', name: 'hello' }, //
        { id: '2', name: 'world' },
        { id: '3', name: 'world' },
      ];

      await db.batchWrite(table, objs);

      await db.deleteAll(table);

      const items = await db.query(table, {});

      expect(items.length).toEqual(0);
    });

    test('count', async () => {
      const objs = [
        { id: '1', name: 'hello' }, //
        { id: '2', name: 'world' },
        { id: '3', name: 'world' },
      ];

      await db.batchWrite(table, objs);

      const count0 = await db.count(table, {});

      expect(count0).toEqual(3);

      await db.deleteAll(table);

      const count1 = await db.count(table, {});

      expect(count1).toEqual(0);
    });
  });

  describe('query', () => {
    const table = 'memos';
    const objs = [
      { id: '1', name: 'helloo', type: 'X', user: { name: 'he' }, age: 20 },
      { id: '2', name: 'world3', type: 'Y', user: { name: 'wd' }, age: 210 },
      { id: '3', name: 'world1', type: 'Z', user: { name: 'wd' }, age: -2 },
      { id: '4', name: 'world2', type: 'X', user: { name: 'wd' }, age: 2 },
      { id: '5', name: 'AAAAAA', type: 'Y', user: { name: 'AA' }, age: 20.1 },
    ];

    // 比較用に理想の動作をする query 関数
    // const query = (objs: any[], filter: any, sorts?: [string, string][]) => {
    const query = (objs: any[], params: FindParams) => {
      const { filter, sort } = params;
      const _sort = sort ? sort[0] : [];
      const sortKey = _sort ? _sort[0] : 'id';
      const sortOrder = _sort ? _sort[1] : 'DESC';

      const compare = (o: any) => (m: boolean, v: any, k: string): boolean => {
        let f = true;
        if (_.isArray(v)) {
          f = v.includes(o[k]);
        } else if (/%$/.test(k)) {
          const key = k.replace('%', '');
          const str = _.get(o, key);
          return str && str.indexOf(v) === 0;
        } else {
          f = _.isEqual(v, _.get(o, k));
        }
        return m && f;
      };

      const result = _.sortBy(
        _.filter(objs, (o) => {
          return _.reduce(filter, compare(o), true);
        }),
        [sortKey, 'id'], // 第二キー:id
      );
      sortOrder === 'DESC' && _.reverse(result);
      return params.limit ? result.slice(0, params.limit) : result;
    };

    beforeAll(async () => {
      await db.deleteAll(table);
      await db.batchWrite(table, objs);
    });

    test('フィルタ', async () => {
      const params: FindParams = {
        filter: { type: 'X' },
      };
      const _received = await db.query(table, params);
      const received = _.sortBy(_received, 'id');
      const _expected = query(objs, params);
      const expected = _.sortBy(_expected, 'id');
      expect(received).toEqual(expected);
    });

    test('フィルタ IN', async () => {
      const params: FindParams = {
        filter: { type: ['X', 'Y'] },
        sort: [['name', 'ASC']],
      };
      const received = await db.query(table, params);
      const expected = query(objs, params);
      expect(received).toEqual(expected);
    });

    test('フィルタ IN', async () => {
      const params: FindParams = {
        filter: { type: ['Y', 'X'] },
        sort: [['type', 'DESC']],
      };
      const received = await db.query(table, params);
      const expected = query(objs, params);
      expect(received).toEqual(expected);
    });

    test('フィルタ 階層', async () => {
      const params: FindParams = {
        filter: { 'user.name': 'wd' },
      };
      const _received = await db.query(table, params);
      const received = _.sortBy(_received, 'id');
      const _expected = query(objs, params);
      const expected = _.sortBy(_expected, 'id');
      expect(received).toEqual(expected);
    });

    test('フィルタ 前方一致', async () => {
      const params: FindParams = {
        filter: { 'name%': 'world' },
      };
      const _received = await db.query(table, params);
      const received = _.sortBy(_received, 'id');
      const _expected = query(objs, params);
      const expected = _.sortBy(_expected, 'id');
      expect(received).toEqual(expected);
    });

    test('ソート 文字列', async () => {
      const params: FindParams = {
        sort: [['name', 'ASC']],
      };
      const received = await db.query(table, params);
      const expected = query(objs, params);
      expect(received).toEqual(expected);
    });

    test('ソート 文字列 リミット', async () => {
      const params: FindParams = {
        sort: [['name', 'ASC']],
        limit: 3,
      };
      const received = await db.query(table, params);
      const expected = query(objs, params);
      expect(received).toEqual(expected);
    });

    test('ソート 数値', async () => {
      const params: FindParams = {
        sort: [['age', 'ASC']],
      };
      const received = await db.query(table, params);
      const expected = query(objs, params);
      expect(received).toEqual(expected);
    });

    test('フィルタ ＆ ソート', async () => {
      const params: FindParams = {
        filter: { 'name%': 'world' },
        sort: [['age', 'ASC']],
      };
      const received = await db.query(table, params);
      const expected = query(objs, params);
      expect(received).toEqual(expected);
    });
  });
});
