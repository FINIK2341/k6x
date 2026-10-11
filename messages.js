import {
  SchemaRegistry,
  SCHEMA_TYPE_JSON,
} from "k6/x/kafka";
import * as utils from '/home/test/Genk6/utils/utils.js';

const schemaRegistry = new SchemaRegistry();

export function Prep(rqUid, operDt, dateTime, operUid0, rqTm, rqOperUid0, stepTm, product, date){
  let stepTm0,
      stepTm1,
      stepTm2,
      stepTm3,
      stepTm4,
      stepTm5,
      message;

  stepTm0 = utils.StepTm(stepTm);
  utils.Plus100Ms(stepTm);
  stepTm1 = utils.StepTm(stepTm);
  utils.Plus100Ms(stepTm);
  stepTm2 = utils.StepTm(stepTm);
  utils.Plus100Ms(stepTm);
  stepTm3 = utils.StepTm(stepTm);
  utils.Plus100Ms(stepTm);
  stepTm4 = utils.StepTm(stepTm);
  utils.Plus100Ms(stepTm);
  stepTm5 = utils.StepTm(stepTm);
  utils.Plus100Ms(stepTm);
  message = [
		{
        },
        schemaType: SCHEMA_TYPE_JSON,
      }),
      headers: {
        
      },
    },
  ];
  return message;
}
