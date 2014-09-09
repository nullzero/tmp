from sqlalchemy import *
from sqlalchemy.orm import scoped_session, sessionmaker, relationship, backref
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

LENGTH_DIFF = 500

ts90days = tots(datetime.datetime.now() - datetime.timedelta(days=1))
ts30days = tots(datetime.datetime.now() - datetime.timedelta(days=1))

dic = {}
users = User.query.filter(User.user_registration >= ts90days).all()
print 'start the loop'

import operator

with open('query-result.txt', 'w') as f:
    for user in users:
        cnt = 0
        print user
        try:
            revisions = user.revisions
            for revision in revisions:
                print revision
                """
                if revision.rev_timestamp >= ts30days and revision.page.page_namespace == 0 and revision.rev_deleted == 0:
                    if revision.parent is None:
                        if revision.rev_len >= LENGTH_DIFF:
                            cnt += 1
                    else:
                        if revision.parent.rev_len - revision.rev_len >= LENGTH_DIFF:
                            cnt += 1
                """
            if cnt:
                dic[user.user_name] = cnt
        except Exception as e:
            f.write('found problem with {}'.format(user.user_name))
            print e
            print 'found problem with {}'.format(user.user_name)

    for row in sorted(x.iteritems(), key=operator.itemgetter(1), reverse=True):
        f.write('| [[User:{}]] || {}\n'.format(row[0], row[1]))
