module.exports = {
  verbose: true,
  testEnvironment: 'node',
  moduleFileExtensions: ['ts', 'tsx', 'js'],
  transform: {
    '^.+\\.(ts|tsx)$': 'ts-jest',
  },
  globals: {
    'ts-jest': {
      tsConfig: 'tsconfig.json',
    },
  },
  testMatch: ['**/test/**/*.spec.+(ts|tsx|js)'],
  reporters: [
    'default',
    [
      'jest-junit',
      { outputDirectory: 'TestResults', outputName: 'TEST-RESULT.xml' },
    ],
  ],
  coverageDirectory: 'TestResults',
  coverageReporters: ['text', 'html', 'cobertura'],
  setupFilesAfterEnv: ['./jest.setup.js'],
};
