import logging
import nzpy
import pathlib
import pytest
import io

@pytest.fixture
def lf(request):
    f = pathlib.Path("nzpy.log")
    if f.exists():
        f.unlink()
    try:
        yield f
    finally:
        if f.exists():
            f.unlink()

@pytest.fixture
def db(db_kwargs):
    db_kwargs['database'] = 'system'
    return db_kwargs

@pytest.fixture
def base_log(request):
    for l in logging.getLogger().handlers.copy():
        logging.getLogger().removeHandler(l)
    lbuf = io.StringIO()

    s = logging.StreamHandler(lbuf)
    s.setFormatter(logging.Formatter('%(name)s %(message)s'))
    logging.getLogger().addHandler(s)
    return lbuf

def test_parent_logging(db, lf, base_log):
    db['logOptions'] = nzpy.LogOptions.Inherit
    nzpy.connect(**db)

    # log file shuoldn't be created
    assert not lf.exists()
    # parent should have its logs
    assert 'nzpy' in base_log.getvalue()
    
def test_parent_logging_negative(db, lf, base_log):
    db['logLevel'] = logging.INFO

    for i in [logging.getLogger()] + logging.getLogger().handlers:
        i.setLevel(logging.CRITICAL)

    logging.critical("hello")
    nzpy.connect(**db)
    # log file shuoldn't be created
    assert not lf.exists()
    # parent shouldn't have its logs
    assert 'nzpy' not in base_log.getvalue()

def test_parent_logging_disabled(db, lf, base_log):
    #db['log_option'] = ~(nzpy.LogOptions.Inherit|nzpy.LogOptions.Logfile)
    db['logOptions'] = nzpy.LogOptions.Disabled
    db['logLevel'] = logging.INFO

    logging.basicConfig()
    logging.error("hello")
    nzpy.connect(**db)
    # log file shuoldn't be created
    assert not lf.exists()
    # parent shouldn't have its logs
    assert 'nzpy' not in base_log.getvalue()

def test_log_file(db, lf, base_log):
    db['logOptions'] = nzpy.LogOptions.Logfile
    nzpy.connect(**db)

    assert lf.exists()
    # parent should have its logs
    assert 'nzpy' not in base_log.getvalue()
    nzpy.connect(**db)


