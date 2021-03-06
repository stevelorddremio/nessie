/* tslint:disable */
/* eslint-disable */
/**
 * Nessie API
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.5.0
 *
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */

import { exists, mapValues } from '../runtime';
import {
    ContentsKey,
    ContentsKeyFromJSON,
    ContentsKeyFromJSONTyped,
    ContentsKeyToJSON,
} from './';

/**
 *
 * @export
 * @interface Delete
 */
export interface Delete {
    /**
     *
     * @type {ContentsKey}
     * @memberof Delete
     */
    key?: ContentsKey;
}

export function DeleteFromJSON(json: any): Delete {
    return DeleteFromJSONTyped(json, false);
}

export function DeleteFromJSONTyped(json: any, ignoreDiscriminator: boolean): Delete {
    if ((json === undefined) || (json === null)) {
        return json;
    }
    return {

        'key': !exists(json, 'key') ? undefined : ContentsKeyFromJSON(json['key']),
    };
}

export function DeleteToJSON(value?: Delete | null): any {
    if (value === undefined) {
        return undefined;
    }
    if (value === null) {
        return null;
    }
    return {

        'key': ContentsKeyToJSON(value.key),
    };
}
