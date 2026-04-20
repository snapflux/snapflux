/** @type {import('jest').Config} */
module.exports = {
  moduleFileExtensions: ['js', 'json', 'ts'],
  rootDir: '.',
  testRegex: 'test/.*\\.spec\\.ts$',
  transform: { '^.+\\.(t|j)s$': ['ts-jest', {
    tsconfig: {
      emitDecoratorMetadata: true,
      experimentalDecorators: true,
    },
  }] },
  testEnvironment: 'node',
};
