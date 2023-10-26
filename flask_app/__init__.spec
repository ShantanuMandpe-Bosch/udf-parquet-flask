# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['__init__.py'],
    pathex=['C:\\Users\\MSH5RT\\.conda\\envs\\udfenv\\Lib\\site-packages'],
    binaries=[],
    datas=[('C:\\Users\\MSH5RT\\Desktop\\udf\\udf-flask\\flask-file-system\\flask_app\\staticFiles', 'staticFiles'), ('C:\\Users\\MSH5RT\\Desktop\\udf\\udf-flask\\flask-file-system\\flask_app\\templates', 'templates'), ('C:\\Users\\MSH5RT\\Desktop\\udf\\udf-flask\\flask-file-system\\flask_app\\UDFDecoder', 'UDFDecoder')],
    hiddenimports=['numpy','pyarrow','pyarrow.vendored.version'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='__init__',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
