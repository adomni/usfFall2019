import {Logger} from '@bitblit/ratchet/dist/common/logger';
import {ReflectiveInjector} from 'injection-js';
import * as moment from 'moment';
import {UsfFall2019Container} from '../../usf-fall-2019-container';
import {Tae} from '../../student/tae';

const container: ReflectiveInjector = UsfFall2019Container.getContainer();
const inst: Tae = container.get(Tae);

Logger.info('Starting at %s', moment().format('YYYY-MM-DD-HH-mm-SS'));
inst.doStuff().then(result => {
    Logger.info('Processor returned : %s', result);
}).catch(err => {
    Logger.error('Something went wrong : %s', err, err);
})
