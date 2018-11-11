from datacleaner import data_prep

INPUT_DATA = {
    'd': '01/1/2010',
    'fn': 'George',
    'ln': 'Washington',
    'e': 'wizard@gmail.com',
    'a1': '123 Shilling Street',
    'a2': 'Seattle',
    'a3': 'Washington',
    'a4': '12345'
}

VALID_RESULT = {
    'dob': '01/1/2010',
    'n': 'George Washington',
    'e': 'wizard',
    'd': 'gmail.com',
    'a': '123 Shilling Street Seattle Washington 12345'
}


def test_data_prep_misc():
    """Validate 'x' fields are removed."""
    source = INPUT_DATA.copy()
    source['x1'] = 'unknown'
    source['x'] = 'another unknown'
    assert data_prep(source) == VALID_RESULT


def test_data_prep_empty():
    """Validate empty fields are removed."""
    source = INPUT_DATA.copy()
    source['blah'] = ''
    source['other'] = 'null'
    source['empty'] = 'blank'
    assert data_prep(source) == VALID_RESULT


def test_data_prep_addresses():
    """Validate address fields are combined to one 'a' field."""
    source = INPUT_DATA.copy()
    assert data_prep(source)['a'] == VALID_RESULT['a']


def test_data_prep_names():
    """Validate first and last name fields are combined to one 'n' field."""
    source = INPUT_DATA.copy()
    assert data_prep(source)['n'] == VALID_RESULT['n']


def test_data_prep_dob():
    """Validate date of birth 'd' field is renamed to 'dob'."""
    source = INPUT_DATA.copy()
    assert data_prep(source)['dob'] == VALID_RESULT['dob']


def test_data_prep_email():
    """Validate domain of email address is split out to 'd' field."""
    source = INPUT_DATA.copy()
    result = data_prep(source)
    assert result.get('e') == VALID_RESULT['e']
    assert result.get('d') == VALID_RESULT['d']
