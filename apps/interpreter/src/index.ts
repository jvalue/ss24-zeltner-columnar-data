// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { DebugGranularityValues } from '@jvalue/jayvee-execution';
import { Command } from 'commander';

import { version as packageJsonVersion } from '../package.json';

import { assertNodeVersion } from './prerequisites';
import { runAction } from './run-action';

const runtimeParameterRegex = /^([_a-zA-Z][\w_]*)=(.*)$/;
function collectRuntimeParameters(
  optionValue: string,
  previous: Map<string, string>,
): Map<string, string> {
  const regexMatch = optionValue.match(runtimeParameterRegex);
  if (regexMatch == null) {
    throw new Error(
      `Encountered runtime parameter with invalid syntax: ${optionValue}`,
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const parameter = regexMatch[1]!;
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const value = regexMatch[2]!;

  previous.set(parameter, value);
  return previous;
}

assertNodeVersion();
const program = new Command();

const version: string = packageJsonVersion;
program.version(version);

program
  .argument('<file>', `path to the .jv source file`)
  .option(
    '-e, --env <parameter>=<value>',
    'provide a runtime parameters',
    collectRuntimeParameters,
    new Map<string, string>(),
  )
  .option('-d, --debug', 'enable debug logging', false)
  .option(
    '-dg, --debug-granularity <granularity>',
    `Sets the granularity of block debug logging. Can have values ${DebugGranularityValues.join(
      ', ',
    )} (default).`,
    DebugGranularityValues[DebugGranularityValues.length - 1],
  )
  .option(
    '-dt, --debug-target <block name>',
    `Sets the target blocks of the of block debug logging, separated by comma. If not given, all blocks are targeted.`,
    undefined,
  )
  .option(
    '-po, --parse-only',
    'Only parses the model without running it. Exits with 0 if the model is valid, with 1 otherwise.',
    false,
  )
  .option(
    '--use-polars',
    'WARNING: THIS IS EXPERIMENTAL AND PROBABLY BROKEN: Use the experimental polars backend instead of the typescript native implementation.',
    false,
  )
  .option(
    '--use-rusqlite',
    'WARNING: THIS IS EXPERIMENTAL AND PROBABLY BROKEN: Use the experimental polars backend and the experimental rust sqlite loader.',
  )
  .description('Run a Jayvee file')
  .action(runAction);

program.showHelpAfterError();

program.parse(process.argv);
