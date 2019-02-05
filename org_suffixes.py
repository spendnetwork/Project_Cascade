global org_type_dict
org_type_dict = {
    'società a responsabilità limitata semplificata': 'srls',
    'società\' a responsabilità\' limitata semplificata': 'srls',
    'societa a responsabilita limitata semplificata': 'srls',
    'societa\' a responsabilita\' limitata semplificata': 'srls',
    'società a responsabilità limitata': 'srl',
    'società\' a responsabilità\' limitata': 'srl',
    'societa a responsabilita limitata': 'srl',
    'societa\' a responsabilita\' limitata': 'srl',
    's r l': 'srl',
    ' srl ': ' srl ',  # srl - added whitespace
    ' srl': ' srl',  # srl - added whitespace left (for EO string)
    's. r. l.': 'srl',
    's r l s': 'srls',
    ' srls': ' srls',  # whitespace left
    ' srls ': ' srls ',   # whitespace
    's. r. l. s.': 'srls',
    'società per azioni': 'spa',
    'societa per azioni': 'spa',
    's p a': 'spa',
    's. p. a.': 'spa',
    ' spa ': ' spa ',  # whitespace
    ' spa': ' spa',  # whitespace left
    'Società in nome collettivo': 'snc',
    'Societa in nome collettivo': 'snc',
    's n c': 'snc',
    ' snc ': ' snc ',  # whitespace
    ' snc': ' snc',  # whitespace left
    's. n. c.': 'snc',
    'società in accomandita semplice': 'sas',
    'societa in accomandita semplice': 'sas',
    's a s': 'sas',
    's. a. s.': 'sas',
    ' sas ': ' sas ',  # whitespace
    ' sas': ' sas',  # whitespace left
    'societa cooperativa sociale': 'scs',
    'società cooperativa sociale': 'scs',
    's c s': 'scs',
    's. c. s.': 'scs',
    ' scs ': 'scs',
    ' scs': 'scs'}
