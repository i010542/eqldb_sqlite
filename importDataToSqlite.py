from pythonAPI.eqldb import EqlDB
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, ForeignKey, Index
from sqlalchemy.orm import relationship
import os

Base = declarative_base()


class SPO(Base):
    __tablename__ = 'spo'
    factID = Column(Text, primary_key=True, index=True)
    s = Column(Text, index=True)
    p = Column(Text, index=True)
    o = Column(Text, index=True)

    qvs = relationship('QV', backref='spo', lazy='dynamic')


class QV(Base):
    __tablename__ = 'qv'
    factID = Column(Text, ForeignKey("spo.factID"), index=True, primary_key=True)
    q = Column(Text, index=True, primary_key=True)
    v = Column(Text, index=True, primary_key=True)


Index('idx_spo_id_s', SPO.factID, SPO.s)
Index('idx_spo_id_p', SPO.factID, SPO.p)
Index('idx_spo_id_o', SPO.factID, SPO.o)
Index('idx_spo_s_p', SPO.s, SPO.p)
Index('idx_spo_s_o', SPO.s, SPO.o)
Index('idx_spo_p_o', SPO.p, SPO.o)

Index('idx_spo_id_s_p', SPO.factID, SPO.s, SPO.p)
Index('idx_spo_id_s_o', SPO.factID, SPO.s, SPO.o)
Index('idx_spo_id_p_o', SPO.factID, SPO.p, SPO.o)
Index('idx_spo_s_p_o', SPO.s, SPO.p, SPO.o)

Index('idx_spo_id_s_p_o', SPO.factID, SPO.s, SPO.p, SPO.o)

Index('idx_qv_id_q', QV.factID, QV.q)
Index('idx_qv_id_v', QV.factID, QV.v)
Index('idx_qv_q_v', QV.q, QV.v)
Index('idx_qv_id_q_v', QV.factID, QV.q, QV.v)


def init():
    if os.path.exists("./dbs/"):
        pass
    else:
        os.mkdir("./dbs")

    if os.path.exists("./dbs/eqldb_db.db"):
        pass
    else:
        engine = create_engine('sqlite:///dbs/eqldb_db.db')
        Base.metadata.create_all(engine, checkfirst=True)


def fillInDatabase(esDBName, sqliteDBName):
    db = EqlDB('lemon.net.cn', 8086, esDBName, '', '')

    res = db.execute("?x:所获奖项:诺贝尔文学奖(?y)")[1]
    engine = create_engine('sqlite:///dbs/' + sqliteDBName + '.db')
    Session = sessionmaker(bind=engine)
    session = Session()

    for r in res:
        x = r.get('?x')
        y = r.get('?y')
        factID = r.get('?factID')
        print(factID, x, '所获奖项', '诺贝尔文学奖')
        newSPO = SPO(factID=factID, s=x, p='所获奖项', o='诺贝尔文学奖')
        session.add(newSPO)

        for yy in y:
            yy = eval(yy)
            for yyy in yy.items():
                key = yyy[0]
                value = yyy[1]
                print(factID, key, value)
                newQV = QV(factID=factID, q=key, v=value)
                session.add(newQV)
    session.commit()


if __name__ == '__main__':
    init()



