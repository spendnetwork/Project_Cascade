global org_type_dict
org_type_dict = {
    'società a responsabilità limitata semplificata': 's.r.l.s.',
    'società\' a responsabilità\' limitata semplificata': 's.r.l.s.',
    'societa a responsabilita limitata semplificata': 's.r.l.s.',
    'societa\' a responsabilita\' limitata semplificata': 's.r.l.s.',
    'società a responsabilità limitata': 's.r.l.',
    'società\' a responsabilità\' limitata': 's.r.l.',
    'societa a responsabilita limitata': 's.r.l.',
    'societa\' a responsabilita\' limitata': 's.r.l.',
    's r l': 's.r.l.',
    ' srl ': ' s.r.l. ',  # srl - added whitespace
    ' srl': ' s.r.l.',  # srl - added whitespace left (for EO string)
    's. r. l.': 's.r.l.',
    's r l s': 's.r.l.s.',
    ' srls': ' s.r.l.s.',  # whitespace left
    ' srls ': ' s.r.l.s. ',   # whitespace
    's. r. l. s.': 's.r.l.s.',
    'società per azioni': 's.p.a.',
    'societa per azioni': 's.p.a.',
    's p a': 's.p.a.',
    's. p. a.': 's.p.a.',
    ' spa ': ' s.p.a. ',  # whitespace
    ' spa': ' s.p.a.',  # whitespace left
    'Società in nome collettivo': 's.n.c.',
    'Societa in nome collettivo': 's.n.c.',
    's n c': 's.n.c.',
    ' snc ': ' s.n.c. ',  # whitespace
    ' snc': ' s.n.c.',  # whitespace left
    's. n. c.': 's.n.c.',
    'società in accomandita semplice': 's.a.s.',
    'societa in accomandita semplice': 's.a.s.',
    's a s': 's.a.s.',
    's. a. s.': 's.a.s.',
    ' sas ': ' s.a.s. ',  # whitespace
    ' sas': ' s.a.s.',  # whitespace left
    'società in accomandita semplice': 's.a.s.',
    'societa in accomandita semplice': 's.a.s.',
    's a s': 's.a.s.',
    's. a. s.': 's.a.s.',
    ' sas ': ' s.a.s. ',  # whitespace
    ' sas': ' s.a.s.',  # whitespace left
    'societa cooperativa sociale': 's.c.s.',
    'società cooperativa sociale': 's.c.s.',
    's c s': 's.c.s.',
    's. c. s.': 's.c.s.',
    ' scs ': 's.c.s.',
    ' scs': 's.c.s.'}
