import {Injectable} from 'injection-js';
import 'reflect-metadata';
import * as AWS from 'aws-sdk';
import {DynamoRatchet} from '@bitblit/ratchet/dist/aws/dynamo-ratchet';
import * as parse from 'csv-parse/lib/sync';
import {StaticContentFinder} from '../static-content-finder';
import {BooleanRatchet} from '@bitblit/ratchet/dist/common/boolean-ratchet';
import {NumberRatchet} from '@bitblit/ratchet/dist/common/number-ratchet';
import {StringRatchet} from '@bitblit/ratchet/dist/common/string-ratchet';
import {UsfFall2019Constants} from '../usf-fall-2019-constants';


/**
 * Service for functions common to audiences
 */
@Injectable()
export class UsfAudienceService {


    private audienceSegmentCache: AudienceSegment[];
    private audienceSizeDistributionCache: AudienceSizeDistribution[];

    constructor(private ddb: AWS.DynamoDB.DocumentClient, private ratchet: DynamoRatchet,
                private staticContentFinder: StaticContentFinder) {
    }

    public fetchAllAudienceSizeDistributions(): AudienceSizeDistribution[] {
        if (!this.audienceSizeDistributionCache) {
            this.audienceSizeDistributionCache = this.staticContentFinder.fetchAsObject<AudienceSizeDistribution[]>('audience-size-distribution.json')
        }
        return this.audienceSizeDistributionCache;
    }

    public async fetchAudienceSizeDistribution(segmentId: number): Promise<AudienceSizeDistribution> {
        const all: AudienceSizeDistribution[] = this.fetchAllAudienceSizeDistributions();
        return all.find(i => i.segmentId === segmentId);
    }

    public async pctOfAudienceSegmentDistribution(segmentId: number, cnt: number): Promise<number> {
        const ad: AudienceSizeDistribution = await this.fetchAudienceSizeDistribution(segmentId);
        if (!ad) {
            throw new Error('No such segment : ' + segmentId);
        }
        let idx: number = 0;
        while (ad.percentileCutoff[idx + 1] < cnt && idx < 100) {
            idx++;
        }
        return idx / 100;
    }

    public async fetchAudienceSegments(includeIgnored: boolean = false): Promise<AudienceSegment[]> {
        if (!this.audienceSegmentCache) {
            // Using .dat here because *.csv is .gitignored
            const csv: string = this.staticContentFinder.fetchAsString('audience-segments.csv.dat');
            const csvParsed: any[] = parse(csv, {columns: false, skip_empty_lines: true});

            let segments: AudienceSegment[] = [];
            for (let i = 1; i < csvParsed.length; i++) {
                const c: any[] = csvParsed[i];
                const next: AudienceSegment = {
                    id: NumberRatchet.safeNumber(c[0]),
                    category: c[1],
                    group: c[2],
                    label: c[3],
                    usedInTopAudiences: BooleanRatchet.parseBool(c[4]),
                    usedInSummary: BooleanRatchet.parseBool(c[5]),
                    ignored: BooleanRatchet.parseBool(c[6]),
                    sortOrder: NumberRatchet.safeNumber(c[7]),
                    placeIqId: c[8]
                };
                segments.push(next);
            }
            this.audienceSegmentCache = segments;
        }

        if (includeIgnored) {
            return this.audienceSegmentCache;
        } else {
            return this.audienceSegmentCache.filter(s => !s.ignored);
        }

    }

    public async fetchAudienceSegmentByPlaceIqId(pid: String, includeIgnored: boolean = false): Promise<AudienceSegment> {
        const all: AudienceSegment[] = await this.fetchAudienceSegments(includeIgnored);
        return all.find(a => a.placeIqId === pid);
    }

    public async fetchAudienceSegmentById(id: number, includeIgnored: boolean = false): Promise<AudienceSegment> {
        const all: AudienceSegment[] = await this.fetchAudienceSegments(includeIgnored);
        return all.find(a => a.id === id);
    }


    // Unique element count : There is an assumption in here that different boards have different unique devices
    public async fetchAllEntriesForLocation(locationHash: string): Promise<FullLocationAudienceDataElement[]> {
        if (StringRatchet.trimToNull(locationHash) === null) {
            return [];
        } else {
            const qry: any = {
                TableName: UsfFall2019Constants.DDB_CURRENT_AUDIENCE_TABLE,
                KeyConditionExpression: 'locationHash = :locationHash',
                ScanIndexForward: false,
                ExpressionAttributeValues: {
                    ':locationHash': locationHash
                }
            };
            const segments: AudienceSegment[] = await this.fetchAudienceSegments();

            const unfiltered: FullLocationAudienceDataElement[] = await this.ratchet.fullyExecuteQuery<FullLocationAudienceDataElement>(qry);
            const filtered: FullLocationAudienceDataElement[] = unfiltered.filter(e => {
                const seg: AudienceSegment = segments.find(s => s.id === e.audienceSegmentId && !s.ignored);
                return !!seg && e.entryType === 'LOCATION_SUMMARY';
            });

            return filtered;
        }
    }

    /* Requires a full table scan
    public async buildAudienceSizeDistribution(): Promise<AudienceSizeDistributionHolder[]> {
        const scanParams: any = {
            TableName: Usf2019ServerConstants.DDB_CURRENT_AUDIENCE_TABLE
        };
        const rval: Map<number, AudienceSizeDistributionHolder> = new Map<number, AudienceSizeDistributionHolder>();
        let rows: number = 0;

        do {
            const results: PromiseResult<ScanOutput, AWSError> = await this.ddb.scan(scanParams).promise();
            const cast: FullLocationAudienceDataElement[] = results.Items as FullLocationAudienceDataElement[];
            rows += cast.length;
            cast.forEach(s => {
                let cur: AudienceSizeDistributionHolder = rval.get(s.audienceSegmentId);
                if (!cur) {
                    cur = {
                        segmentId: s.audienceSegmentId,
                        counts: []
                    };
                    rval.set(s.audienceSegmentId, cur);
                }
                cur.counts.push(s.uniqueDevicesAtLocation);
            });

            let pct: string = ((rows * 100) / 27965711).toFixed(2);
            Logger.info('Processed %d rows, %s pct', rows, pct);

            scanParams['ExclusiveStartKey'] = results.LastEvaluatedKey;
        } while (!!scanParams['ExclusiveStartKey']);

        const temp: AudienceSizeDistributionHolder[] = Array.from(rval.values());

        Logger.info('Finished');
        return temp;
    }
     */

}

export interface AudienceSizeDistribution {
    segmentId: number;
    percentileCutoff: number[];
    min: number;
    average: number;
    max: number;
    count: number;
    sum: number;
}


export interface AudienceSegment {
    id: number;
    label: string;
    category: string;
    group: string;
    usedInTopAudiences: boolean;
    usedInSummary: boolean;
    ignored: boolean;
    sortOrder: number;
    placeIqId: string;
}


export interface FullLocationAudienceDataElement {
    audienceSegmentId: number
    placeIqId: string
    count: number
    dmaIndex: number
    dmaName: string
    entryType: string
    locationHash: string
    uniqueDevicesAtLocation: number
    generatedEpochMS: number
    notes: string
}