import * as fs from 'fs';
import * as path from 'path';
import {Logger} from '@bitblit/ratchet/dist/common/logger';

export class UsfFall2019Constants {
    public static PACIFIC_TIME_ZONE: string = 'America/Los_Angeles';
    //public static readonly CURRENT_ATHENA_TABLE_PREFIX: string = 'hist_20190520'; //0308';
    public static readonly DDB_CURRENT_AUDIENCE_TABLE: string = 'location-audience-2019-08-17';
    private static BUILD_INFO: any;

    private constructor() {
    }

    public static getBuildInfo(): any {
        if (!UsfFall2019Constants.BUILD_INFO) {
            const pathToRead: string = path.join(__dirname, 'static', 'build-properties.json');
            Logger.info('Reading build info from %s (dirname was %s)', pathToRead, __dirname);
            UsfFall2019Constants.BUILD_INFO = JSON.parse(fs.readFileSync(pathToRead).toString());
        }
        return UsfFall2019Constants.BUILD_INFO;
    }


};
