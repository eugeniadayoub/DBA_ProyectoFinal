#!/usr/bin/env python3
"""
Fix invalid geometries in MongoDB collections using Shapely.
Usage:
  python3 fix_invalid_geometries.py --collection buildings_microsoft --batch-size 2000 [--dry-run]
"""
import os
import sys
import argparse
from pymongo import MongoClient, UpdateOne
from shapely.geometry import shape, mapping, Polygon, MultiPolygon
from shapely.geometry.polygon import orient


def normalize_shapely_geom_obj(g):
    try:
        if g.is_valid:
            return g
    except Exception:
        pass
    try:
        from shapely.ops import make_valid
        g2 = make_valid(g)
        if g2.is_valid:
            return g2
    except Exception:
        try:
            g2 = g.buffer(0)
            if g2.is_valid:
                return g2
        except Exception:
            return None
    try:
        g2 = g.buffer(0)
        if g2.is_valid:
            return g2
    except Exception:
        return None
    return None


def normalize_geojson_geom(geom_json):
    try:
        g = shape(geom_json)
    except Exception:
        return None
    g2 = normalize_shapely_geom_obj(g)
    if g2 is None:
        return None
    try:
        if isinstance(g2, Polygon):
            g2 = orient(g2, sign=1.0)
        elif isinstance(g2, MultiPolygon):
            g2 = MultiPolygon([orient(p, sign=1.0) for p in g2.geoms])
    except Exception:
        pass
    return mapping(g2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo-uri', default=os.getenv('MONGO_URI', 'mongodb://mongo-upme:27017/'))
    parser.add_argument('--db', default=os.getenv('DB_NAME', 'dba_proyectofinal'))
    parser.add_argument('--collection', action='append', help='Collection to process; can be repeated', default=['buildings_microsoft'])
    parser.add_argument('--batch-size', type=int, default=2000)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    for coll_name in args.collection:
        coll = db[coll_name]
        total = coll.count_documents({})
        print(f"Processing collection {coll_name} ({total} documents)")
        cursor = coll.find({}, {'geometry': 1}).batch_size(args.batch_size)
        ops = []
        processed = 0
        fixed = 0
        skipped = 0
        for doc in cursor:
            processed += 1
            geom = doc.get('geometry')
            if not geom:
                skipped += 1
                continue
            new_geom = normalize_geojson_geom(geom)
            if new_geom is None:
                skipped += 1
                continue
            if new_geom != geom:
                fixed += 1
                if not args.dry_run:
                    ops.append(UpdateOne({'_id': doc['_id']}, {'$set': {'geometry': new_geom}}))
            if len(ops) >= args.batch_size:
                if not args.dry_run:
                    coll.bulk_write(ops)
                ops = []
        if ops and not args.dry_run:
            coll.bulk_write(ops)
        print(f"  Processed: {processed}, Fixed: {fixed}, Skipped (no geom or unfixable): {skipped}")

    client.close()

if __name__ == '__main__':
    main()
