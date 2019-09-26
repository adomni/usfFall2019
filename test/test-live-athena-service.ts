import {ReflectiveInjector} from 'injection-js';
import {AthenaService} from '../src/service/athena-service';
import {Row} from 'aws-sdk/clients/athena';
import {UsfFall2019Container} from '../src/usf-fall-2019-container';
import { Logger } from '@bitblit/ratchet/dist/common/logger';


describe('#athenaService', function () {
    this.timeout(30000);

    it('should list queries', async () => {
        Logger.setLevelByName('debug');

        const container: ReflectiveInjector = UsfFall2019Container.getContainer();
        const svc: AthenaService = container.get(AthenaService);

        const qry: string = 'select * from location_data.hist_20190817_billboard_devices where mobile_device_id=\'{LOCATION_HASH_PREFIX}\';';

        const result: Row[] = await svc.runQuery(qry, {LOCATION_HASH_PREFIX:'2a7e1bff-f551-4ee4-a0d9-16d25f99d75e'});
        Logger.info('Got data : %j', result);
        Logger.info('Got %d rows', result.length);

    });


});
