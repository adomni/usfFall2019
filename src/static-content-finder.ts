import {Logger} from '@bitblit/ratchet/dist/common/logger';
import {Injectable} from 'injection-js';
import 'reflect-metadata';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Abstracts the location of static files away
 */
@Injectable()
export class StaticContentFinder {

    constructor() {
    }

    public fetch(inPath: string): Buffer {
        const fullPath: string = this.fetchFullPath(inPath);
        Logger.debug('Fetching file %s from %s', inPath, fullPath);
        const buffer: Buffer = fs.existsSync(fullPath) ? fs.readFileSync(fullPath) : null;
        return buffer;
    }

    public fetchAsString(inPath: string, encoding?: string): string {
        const buf: Buffer = this.fetch(inPath);
        return (buf) ? buf.toString(encoding) : null;
    }

    public fetchAsObject<T>(inPath: string): T {
        const st: string = this.fetchAsString(inPath);
        return (st) ? JSON.parse(st) as T : null;
    }

    public fetchFullPath(inPath: string): string {
        const fullPath: string = path.join(__dirname, 'static', inPath);
        return fullPath;
    }

    public fetchAndFill(inPath: string, filler: any): string {
        let contents: string = this.fetchAsString(inPath);
        if (contents && filler) {
            Object.keys(filler).forEach(key => {
                const value = filler[key];
                Logger.debug('Replacing %s with %s', key, value);
                contents = contents.replace(new RegExp('{{' + key + '}}', 'g'), value);
            });
        }
        return contents;
    }

}
