import AWS from 'aws-sdk';
import DynamoDB from '../src/index';
import _ from 'lodash';

const TableName = 'TestTable';

describe('template.yaml', () => {
  let db: DynamoDB;

  beforeAll(async () => {
    // docker run --rm -p 8000:8000 amazon/dynamodb-local
    AWS.config.update(config);
    const dynamodb = new AWS.DynamoDB();
    await dynamodb
      .deleteTable({ TableName })
      .promise()
      .catch(() => {});
    await dynamodb.createTable(dbparams).promise();

    // ここでテーブルのメタデータを渡す
    // name->_1, key->_2 とかへのローカルインデックスへのマッピング
    db = new DynamoDB(TableName, 10);
    const _metadata_ = [
      { id: 'users', indexes: ['name', 'key'] }, //
      { id: 'groups', indexes: ['name'] },
      { id: 'memos', indexes: ['name'] },
    ];
    await db.batchWrite('_metadata_', _metadata_);
  });

  test('read', async () => {
    const table = 'users';
    const id = 'hello';
    const name = 'WORLD';
    const key = '111';

    const obj = { id, name, key };

    await db.update(table, obj);

    const user = (await db.read(table, id)) || {};
    expect(user.id).toEqual(id);

    const filter = { key };
    const users = await db.query(table, { filter });
    expect(users[0].id).toEqual(id);
  });

  test('batchWrite', async () => {
    const table = 'users';
    const objs = [
      { id: '1', name: 'hello' }, //
      { id: '2' },
      { id: '3' },
    ];

    await db.batchWrite(table, objs);

    for (const obj of objs) {
      const r = await db.read(table, obj.id);
      if (r) {
        expect(r.id).toEqual(obj.id);
      }
    }
  });

  test('batchGet', async () => {
    const table = 'users';
    const objs = [
      { id: '1', name: 'hello' }, //
      { id: '2' },
      { id: '3' },
    ];

    for (const obj of objs) {
      await db.update(table, obj);
    }

    const ids = objs.map((o) => ({ id: o.id }));
    const users = await db.batchGet(table, ids);

    expect(users.length).toEqual(3);
    users.forEach((r, i) => {
      const o = objs.find((o) => o.id === r.id);
      expect(r.id).toEqual(o?.id);
    });
  });

  test('query', async () => {
    const table = 'memos_query';
    const objs = [
      { id: '1', name: 'hello' }, //
      { id: '2', name: 'world' },
      { id: '3', name: 'world' },
      { id: '4', name: 'world' },
      { id: '5', name: 'AAA' },
    ];

    await db.batchWrite(table, objs);

    const filter = { name: 'world' };
    const items = await db.query(table, { filter });

    expect(items.length).toEqual(3);
    items.forEach((r) => {
      expect(r.name).toEqual('world');
    });
  });

  test('query 前方一致', async () => {
    const table = 'memos';
    const objs = [
      { id: '1', name: 'hello' }, //
      { id: '2', name: 'worldAAA' },
      { id: '3', name: 'worldBBB' },
    ];

    await db.batchWrite(table, objs);

    const filter = { ['name%']: 'world' };
    const items = await db.query(table, { filter });

    expect(items.length).toEqual(2);
    items.forEach((r) => {
      expect(r.name.indexOf('world')).toEqual(0);
    });
  });

  test('removeAll', async () => {
    const table = 'memos';
    const objs = [
      { id: '1', name: 'hello' }, //
      { id: '2', name: 'world' },
      { id: '3', name: 'world' },
    ];

    await db.batchWrite(table, objs);

    await db.removeAll(table);

    const items = await db.query(table, {});

    expect(items.length).toEqual(0);
  });

  test('count', async () => {
    const table = 'groups';
    const objs = [
      { id: '1', name: 'hello' }, //
      { id: '2', name: 'world' },
      { id: '3', name: 'world' },
    ];

    await db.batchWrite(table, objs);

    const count0 = await db.count(table, {});

    expect(count0).toEqual(3);

    await db.removeAll(table);

    const count1 = await db.count(table, {});

    expect(count1).toEqual(0);
  });
});

//
//
//

const config = {
  endpoint: 'http://localhost:8000',
  region: 'us-west-2',
  accessKeyId: '_',
  secretAccessKey: '_',
};

const _attr = (AttributeType: string, AttributeName: string) => ({
  AttributeType,
  AttributeName,
});

const _keyAttr = (KeyType: string, AttributeName: string) => ({
  KeyType,
  AttributeName,
});

const _keySchema = (hash: string, range: string) => [
  _keyAttr('HASH', hash),
  _keyAttr('RANGE', range),
];

const _index = (hash: string, range: string) => ({
  IndexName: `${hash}-${range}-index`,
  KeySchema: _keySchema(hash, range),
  Projection: { ProjectionType: 'ALL' },
});

const dbparams = {
  TableName,
  AttributeDefinitions: [
    _attr('S', '_'),
    _attr('S', 'id'),
    _attr('S', '_1'),
    _attr('S', '_2'),
    _attr('S', '_3'),
    _attr('S', '_4'),
    _attr('S', '_5'),
  ],
  KeySchema: _keySchema('_', 'id'),
  LocalSecondaryIndexes: [
    _index('_', '_1'),
    _index('_', '_2'),
    _index('_', '_3'),
    _index('_', '_4'),
    _index('_', '_5'),
  ],
  ProvisionedThroughput: {
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1,
  },
  // ローカルでは使えない
  // TimeToLiveSpecification: {
  //   AttributeName: 'ttl',
  //   Enabled: true,
  // },
};
