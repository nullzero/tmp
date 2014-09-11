from sqlalchemy import *
from sqlalchemy.orm import scoped_session, sessionmaker, relationship, backref, subqueryload
from sqlalchemy.ext.declarative import declarative_base

URI = 'mysql://thwiki.labsdb/thwiki_p?read_default_file=~/replica.my.cnf'
# URI = 'mysql://root:password@localhost/wikidb'

engine = create_engine(URI, convert_unicode=True)
session = scoped_session(sessionmaker(autocommit=False,
                                      autoflush=False,
                                      bind=engine))
Base = declarative_base()
Base.query = session.query_property()
Base.metadata.bind = engine

class Revision(Base):
    __tablename__ = 'revision'
    __table_args__ = (
        {'autoload': True}
    )
    rev_id = Column(INTEGER, primary_key=True)
    rev_page = Column(INTEGER, ForeignKey("page.page_id"))
    rev_user = Column(INTEGER, ForeignKey("user.user_id"))
    rev_parent_id = Column(INTEGER, ForeignKey("revision.rev_id"))
    page = relationship("Page", backref="revisions")
    user = relationship("User", backref="revisions")
    parent = relationship("Revision", remote_side='Revision.rev_id', backref=backref('child', uselist=False))
    
    def __repr__(self):
        return '<Revision %r>' % (self.rev_id)

class Page(Base):
    __tablename__ = 'page'
    __table_args__ = (
        {'autoload': True}
    )
    page_id = Column(INTEGER, primary_key=True)

    def __repr__(self):
        return '<Page %r>' % (self.page_title)

class User(Base):
    __tablename__ = 'user'
    __table_args__ = (
        {'autoload': True}
    )

    user_id = Column(INTEGER, primary_key=True)

    def __repr__(self):
        return '<User %r>' % (self.user_name)

import datetime

def tots(s):
    return s.strftime("%Y%m%d%H%M%S")

# print Revision.query.filter(Revision.rev_timestamp < "20150816184516").all()

import collections

current = datetime.datetime(day=1, month=9, year=2014)
before1month = tots(current - datetime.timedelta(days=30))
before3month = tots(current - datetime.timedelta(days=90))
current = tots(current)
import operator

def find_edit():
    dic = {}
    users = User.query.options(
        subqueryload(User.revisions)
    ).filter(between(User.user_registration, before3month, current)).all()
    print 'start the loop'
    LENGTH_DIFF = 500
    with open('task1.txt', 'w') as f:
        for user in users:
            cnt = 0
            print user
            try:
                revisions = user.revisions
                for revision in revisions:
                    if (revision.page and (before1month <= revision.rev_timestamp <= current) and
                            revision.page.page_namespace == 0 and
                            revision.rev_deleted == 0):
                        if revision.parent is None:
                            if revision.rev_len >= LENGTH_DIFF:
                                cnt += 1
                        else:
                            if revision.parent.rev_len - revision.rev_len >= LENGTH_DIFF:
                                cnt += 1
                if cnt:
                    dic[user.user_name] = cnt
            except Exception as e:
                f.write('found problem with {}\n'.format(user.user_name))
                print e
                print 'found problem with {}\n'.format(user.user_name)

        for row in sorted(dic.iteritems(), key=operator.itemgetter(1), reverse=True):
            f.write('|-\n| [[User:{}]] || {}\n'.format(row[0], row[1]))
            print row

"""
def find_create_rotate():
    cnt = collections.Counter()
    for i in xrange(0, 5600000, 10000):
        print i
        revisions = Revision.query.filter(between(Revision.rev_id, i, i+10000-1),
                                          Revision.rev_parent_id == 0,
                                          Revision.rev_deleted == 0)
        for revision in revisions:
            print revision
            if revision.page and revision.page.page_namespace == 0:
                cnt[revision.rev_user_text] += 1
    
    print 'begin writing'
    with open('task2.txt', 'w') as f:
        for i, j in cnt.most_common(100):
            f.write('|-\n| [[User:{}]] || {}'.format(i, j))
            print '|-\n| [[User:{}]] || {}'.format(i, j)
"""

def find_create():
    cnt = collections.Counter()
    revisions = Revision.query.filter(between(Revision.rev_timestamp, before1month, current),
                                      Revision.rev_parent_id == 0,
                                      Revision.rev_deleted == 0)
    for revision in revisions:
        print revision
        if revision.page and revision.page.page_namespace == 0:
            cnt[revision.rev_user_text] += 1
    
    print 'begin writing'
    with open('task2.txt', 'w') as f:
        for i, j in cnt.most_common(100):
            f.write('|-\n| [[User:{}]] || {}'.format(i, j))
            print '|-\n| [[User:{}]] || {}'.format(i, j)

find_edit()
# find_create()
