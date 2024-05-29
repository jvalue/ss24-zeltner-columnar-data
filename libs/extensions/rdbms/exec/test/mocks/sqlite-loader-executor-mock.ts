// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import assert from 'assert';

import { type BlockExecutorMock } from '@jvalue/jayvee-execution/test';
import sqlite3 from 'sqlite3';
import { type Mock, type Mocked, vi } from 'vitest';

type MockedSqlite3Database = Mocked<sqlite3.Database>;

export class SQLiteLoaderExecutorMock implements BlockExecutorMock {
  private _sqliteClient: MockedSqlite3Database | undefined;

  get sqliteClient(): MockedSqlite3Database {
    assert(
      this._sqliteClient !== undefined,
      'Client not initialized - please call setup() first!',
    );
    return this._sqliteClient;
  }

  setup(
    registerMocks: (
      sqliteClient: MockedSqlite3Database,
    ) => void = defaultSQLiteMockRegistration,
  ) {
    // setup sqlite3 mock
    this._sqliteClient = new sqlite3.Database('test') as MockedSqlite3Database;
    registerMocks(this._sqliteClient);
  }
  restore() {
    // cleanup sqlite3 mock
    vi.clearAllMocks();
  }
}

export function defaultSQLiteMockRegistration(
  sqliteClient: MockedSqlite3Database,
) {
  (sqliteClient.run as Mock).mockImplementation(
    (query: string, callback: (result: unknown, err: Error | null) => void) =>
      callback('Success', null),
  );
}
