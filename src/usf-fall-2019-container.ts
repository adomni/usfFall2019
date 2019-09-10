import {Logger} from '@bitblit/ratchet/dist/common/logger';
import {Provider, ReflectiveInjector} from 'injection-js';
import 'reflect-metadata';
import * as AWS from 'aws-sdk';

import {StaticContentFinder} from './static-content-finder';
import {DynamoRatchet} from '@bitblit/ratchet/dist/aws/dynamo-ratchet';
import {Tuo} from './student/tuo';
import {Tae} from './student/tae';
import {Kei} from './student/kei';
import {UsfAudienceService} from './service/usf-audience-service';

/**
 * Nothing goes in here but the container!  This is important because NONE OF YOUR PROVIDERS CAN IMPORT
 * THIS CLASS (directly or indirectly)!!!  It'll immediately cause a circular dependency if you try.
 *
 */
export class UsfFall2019Container {
    private static CONTAINER: ReflectiveInjector;
    private static PROVIDERS: Provider[];

    private constructor() {
    }

    public static getContainer(): ReflectiveInjector {
        if (!UsfFall2019Container.CONTAINER) {
            Logger.silly('Building container');
            UsfFall2019Container.PROVIDERS = [
                {
                    provide: AWS.S3,
                    multi: false,
                    useFactory: () => new AWS.S3({region: 'us-east-1'})
                },
                {
                    provide: AWS.DynamoDB.DocumentClient,
                    multi: false,
                    useFactory: () => new AWS.DynamoDB.DocumentClient({region: 'us-east-1'})
                },
                {
                    provide: DynamoRatchet,
                    multi: false,
                    useFactory: (awsDDB: AWS.DynamoDB.DocumentClient) => new DynamoRatchet(awsDDB),
                    deps: [AWS.DynamoDB.DocumentClient]
                },
                {
                    provide: AWS.Athena,
                    multi: false,
                    useFactory: () => new AWS.Athena({region: 'us-east-1', apiVersion: '2017-05-18'})
                },

                StaticContentFinder, UsfAudienceService,
                Kei, Tae, Tuo


            ];
            UsfFall2019Container.CONTAINER = ReflectiveInjector.resolveAndCreate(UsfFall2019Container.PROVIDERS);
            Logger.silly('Finished building container');
        }

        return UsfFall2019Container.CONTAINER;
    }

    public static getProviders(): any[] {
        if (!UsfFall2019Container.PROVIDERS) {
            // Initialize it first
            UsfFall2019Container.getContainer();
        }
        return UsfFall2019Container.PROVIDERS;
    }

    public static get(token: any, notFoundValue?: any): any {
        return UsfFall2019Container.getContainer().get(token, notFoundValue);
    }


}
