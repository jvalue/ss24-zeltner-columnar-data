// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { JayveeModel } from '../../ast/generated/ast';
import { type JayveeValidationProps } from '../validation-registry';
import { checkUniqueNames } from '../validation-util';

export function validateJayveeModel(
  model: JayveeModel,
  props: JayveeValidationProps,
): void {
  checkUniqueNames(model.pipelines, props.validationContext);
  checkUniqueNames(model.transforms, props.validationContext);
  checkUniqueNames(model.valuetypes, props.validationContext);
  checkUniqueNames(model.constraints, props.validationContext);
  checkUniqueNames(model.blocktypes, props.validationContext);
  checkUniqueNames(model.constrainttypes, props.validationContext);
  checkUniqueNames(model.iotypes, props.validationContext);
}
