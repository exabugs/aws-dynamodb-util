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
  // TableName,
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

export default dbparams;
