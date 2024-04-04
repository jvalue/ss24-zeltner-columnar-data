// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import * as assert from 'assert';
import { readFileSync } from 'fs';
import * as path from 'path';

import { AstNode, LangiumDocument, ValidationAcceptor } from 'langium';
import { WorkspaceFolder } from 'vscode-languageserver-protocol';

import {
  DefaultOperatorEvaluatorRegistry,
  DefaultOperatorTypeComputerRegistry,
  EvaluationContext,
  JayveeServices,
  type JayveeValidationProps,
  RuntimeParameterProvider,
  ValidationContext,
  WrapperFactory,
} from '../lib';
import { initializeWorkspace } from '../lib/builtin-library/jayvee-workspace-manager';

// eslint-disable-next-line @typescript-eslint/no-empty-function
export const validationAcceptorMockImpl: ValidationAcceptor = () => {};

/**
 * Returns function for reading a jv test asset file from the specified asset root
 * @param assetPath paths to the asset root directory
 * @returns function for reading jv asset file
 */
export function readJvTestAssetHelper(
  ...assetPath: string[]
): (testFileName: string) => string {
  /**
   * Reads the jv test asset file with the given filename from the previously configured asset directory
   * @param testFileName asset filename containing jv code
   * @returns content of asset file
   */
  return (testFileName: string) => {
    const text = readFileSync(
      path.resolve(...assetPath, testFileName),
      'utf-8',
    );
    // Expect the test asset to contain something
    expect(text).not.toBe('');
    return text;
  };
}

export function expectNoParserAndLexerErrors(
  document: LangiumDocument<AstNode>,
) {
  expect(document.parseResult.parserErrors).toHaveLength(0);
  expect(document.parseResult.lexerErrors).toHaveLength(0);
}

export async function loadTestExtensions(
  services: JayveeServices,
  testExtensionJvFiles: string[],
) {
  assert(testExtensionJvFiles.every((file) => file.endsWith('.jv')));
  const extensions: WorkspaceFolder[] = testExtensionJvFiles.map((file) => ({
    uri: path.dirname(file),
    name: path.basename(file),
  }));
  return initializeWorkspace(services, extensions);
}

export function createJayveeValidationProps(
  validationAcceptor: ValidationAcceptor,
): JayveeValidationProps {
  const operatorTypeComputerRegistry =
    new DefaultOperatorTypeComputerRegistry();
  const operatorEvaluatorRegistry = new DefaultOperatorEvaluatorRegistry();
  const wrapperFactory = new WrapperFactory(operatorEvaluatorRegistry);

  return {
    validationContext: new ValidationContext(
      validationAcceptor,
      operatorTypeComputerRegistry,
    ),
    evaluationContext: new EvaluationContext(
      new RuntimeParameterProvider(),
      operatorEvaluatorRegistry,
    ),
    wrapperFactory: wrapperFactory,
  };
}
