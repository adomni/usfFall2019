import {Logger} from '@bitblit/ratchet/dist/common/logger';
import {Injectable} from 'injection-js';
import {AudienceSegment, FullLocationAudienceDataElement, UsfAudienceService} from '../service/usf-audience-service';

@Injectable()
export class Tae {

    constructor(private audSrv: UsfAudienceService) {
    }

    public async doStuff(): Promise<boolean> {
        Logger.info('Starting for tae');

        const allSegments: AudienceSegment[] = await this.audSrv.fetchAudienceSegments();
        Logger.info('Found %d segments', allSegments.length);

        const sampleLocationHash: string = 'e9d221c70d25fb737b348b340d9b0b4c';
        const allEntriesForHash: FullLocationAudienceDataElement[] = await this.audSrv.fetchAllEntriesForLocation(sampleLocationHash);

        Logger.info('Found %d entries for location %s', allEntriesForHash.length, sampleLocationHash);

        return true;
    }

}


