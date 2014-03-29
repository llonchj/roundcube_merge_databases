#!/usr/local/bin/python
"""
Merges users/identities/contacts and contact groups 
from a roundcube database into another

"""

import sys
import logging
import pkg_resources

import MySQLdb

logger = logging.getLogger()

def get(db, table, where=None, processor=None):
    try:
        cur = db.cursor(MySQLdb.cursors.DictCursor)
        query = """SELECT %s FROM %s""" % ("*", table)
        if where:
            query += " WHERE %s" % where
        cur.execute(query)
        rows = cur.fetchall()
        for obj in rows:
            yield obj
    except Exception, exc:
        logger.exception("get error %s for '%s'", exc, query)
        raise 
    finally:
        cur.close()

def insert(db, table, obj):
    cur = db.cursor(MySQLdb.cursors.DictCursor)
    query = "insert into %s (%s) values (%s)" % (table,
            ", ".join(["`%s`" % x for x in obj.keys()]),
            ", ".join(["%s" for x in obj.values()])
        )
    try:
        cur.executemany(query, [obj.values()])
        
    except Exception, exc:
        logger.exception("insert error %s for '%s' with %s", exc, query, obj.values())
        db.rollback()
        raise

    finally:
        l = cur.lastrowid
        cur.close()
        return l
        
def move(db, db2, where=None):
    for user in get(db, "users", where):
        where = "user_id={user_id}".format(**user)
    
        identities = [x for x in get(db, "identities", where)]
        contacts = [x for x in get(db, "contacts", where)]
        contactgroups = [x for x in get(db, "contactgroups", where)]

        new_user_id = None
        del user["user_id"]
        #insert user
        user["user_id"] = insert(db2, "users", user)

        for identity in identities:
            identity["user_id"] = user["user_id"]
        
            del identity["identity_id"]
            insert(db2, "identities", identity)
        
        for contact in contacts:
            contact["user_id"] = user["user_id"]

            old_contact_id = contact["contact_id"]
            del contact["contact_id"]
        
            contact["contact_id"] = insert(db2, "contacts", contact)
            contact["old_contact_id"] = old_contact_id
    
        for contactgroup in contactgroups:
            contactgroup["user_id"] = user["user_id"]
        
            old_contactgroup_id = contactgroup["contactgroup_id"]
            del contactgroup["contactgroup_id"]
        
            contactgroup["contactgroup_id"] = insert(db2, "contactgroups", contactgroup)
            contactgroup["old_contactgroup_id"] = old_contactgroup_id

        def get_contact_key(s, d):
            for i in contacts:
                if i["old_contact_id"] == s:
                    return i[d]
            raise Exception("old_contact_id not found")

        for contactgroup in contactgroups:
            where = "contactgroup_id = {old_contactgroup_id}".format(**contactgroup)
            contactgroupmembers = [x for x in get(db, "contactgroupmembers", where)]
            for contactgroupmember in contactgroupmembers:
                contactgroupmember["contactgroup_id"] = contactgroup["contactgroup_id"]
                try:
                    contactgroupmember["contact_id"] = get_contact_key(contactgroupmember["contact_id"], "contact_id")
                except Exception, exc:
                    logger.exception("get_contact_key error %s for %s with %s", exc, contactgroupmember, contact)
                    raise 
                else:
                    insert(db2, "contactgroupmembers", contactgroupmember)

def main():
    import os
    import optparse
    
    LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

    version = pkg_resources.get_distribution("roundcube_merge_databases").version
    parser = optparse.OptionParser(usage="%prog [options] db_source db_dest", 
                                    version="%prog " + version)
    
    parser.add_option("-H", "--host", dest="db_host", 
                      default=os.environ.get("DB_HOST", "localhost"),
                      help="database hostname", metavar="DB_HOST")
    parser.add_option("-u", "--user", dest="db_user", 
                      default=os.environ.get("DB_USER", "mysql"),
                      help="database user", metavar="DB_USER")
    parser.add_option("-p", "--password", dest="db_password",
                      default=os.environ.get("DB_PASS", "password"),
                      help="database password", metavar="DB_PASS")
    parser.add_option("-w", "--where", dest="where", default=None,
                      help="SQL WHERE clause. (i.e. user_id>=1)", metavar="WHERE")
    parser.add_option("-l", "--log-level", dest="log_level", default="info", 
                      choices=LEVELS.keys(), metavar="LOG_LEVEL",
                      help="Log level: {0}".format(", ".join(LEVELS.keys())))
    parser.add_option("-L", "--log-file", dest="log_file", default=None, 
                      help="Log file", metavar="LOG_FILE")
    (options, args) = parser.parse_args()
    logging.basicConfig(filename=options.log_file,level=LEVELS[options.log_level])
    
    if len(args) != 2:
        sys.stderr.write("Invalid arguments\n")
        parser.print_usage()
        exit(3)

    db_source, db_dest = args

    try:
        db_source = MySQLdb.connect(host=options.db_host, user=options.db_user,
            passwd=options.db_password, db=db_source)
    except Exception, exc:
        logger.exception("Error %s connecting to source database: %s@%s", 
                         exc, db_source, options.db_host)
        exit(2)
        
    try:
        db_dest = MySQLdb.connect(host=options.db_host, user=options.db_user,
            passwd=options.db_password, db=db_dest)
    except Exception, exc:
        logger.exception("Error %s connecting to destination database: %s@%s", 
                         exc, db_dest, options.db_host)
        exit(2)

    exit(1)
    try:
        move(db_source, db_dest, options.where)
    except Exception, exc:
        print exc
        db_dest.rollback()
        exit(1)
    else:
        db_dest.commit()
        exit(0)
    finally:
        db_dest.close()
        db_source.close()

if __name__ == '__main__':
    main()