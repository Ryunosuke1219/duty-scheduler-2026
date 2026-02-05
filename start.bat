@echo off
setlocal enabledelayedexpansion
chcp 65001 > nul
title 当直シフトスケジューラ

set "PYTHON_DIR=%~dp0python"
set "PYTHON_EXE=%PYTHON_DIR%\python.exe"
set "PYTHON_VERSION=3.12.4"
set "PYTHON_URL=https://www.python.org/ftp/python/%PYTHON_VERSION%/python-%PYTHON_VERSION%-embed-amd64.zip"

echo ========================================
echo   当直シフトスケジューラ
echo ========================================
echo.

:: Python環境のチェック
if not exist "%PYTHON_EXE%" (
    echo [初回セットアップ] Python環境を構築しています...
    echo.

    :: pythonフォルダ作成
    if not exist "%PYTHON_DIR%" mkdir "%PYTHON_DIR%"

    :: Python埋め込み版のダウンロード
    echo Python %PYTHON_VERSION% をダウンロード中...
    powershell -Command "Invoke-WebRequest -Uri '%PYTHON_URL%' -OutFile '%PYTHON_DIR%\python.zip'"
    if errorlevel 1 (
        echo エラー: Pythonのダウンロードに失敗しました
        echo インターネット接続を確認してください
        pause
        exit /b 1
    )

    :: 展開
    echo 展開中...
    powershell -Command "Expand-Archive -Path '%PYTHON_DIR%\python.zip' -DestinationPath '%PYTHON_DIR%' -Force"
    del "%PYTHON_DIR%\python.zip"

    :: pip有効化（._pthファイルを編集してimport siteを有効化）
    echo pip を有効化中...
    for %%f in ("%PYTHON_DIR%\python*._pth") do (
        powershell -Command "(Get-Content '%%f') -replace '#import site','import site' | Set-Content '%%f'"
    )

    :: get-pipでpipインストール
    echo pip をインストール中...
    powershell -Command "Invoke-WebRequest -Uri 'https://bootstrap.pypa.io/get-pip.py' -OutFile '%PYTHON_DIR%\get-pip.py'"
    "%PYTHON_EXE%" "%PYTHON_DIR%\get-pip.py" --no-warn-script-location
    del "%PYTHON_DIR%\get-pip.py"

    echo.
    echo [初回セットアップ] 依存パッケージをインストールしています...
    echo （数分かかる場合があります）
    echo.

    :: 依存パッケージのインストール
    "%PYTHON_EXE%" -m pip install --no-warn-script-location -r "%~dp0requirements.txt"
    if errorlevel 1 (
        echo エラー: パッケージのインストールに失敗しました
        pause
        exit /b 1
    )

    echo.
    echo セットアップが完了しました！
    echo.
)

:: Streamlitアプリの起動
echo Webアプリを起動しています...
echo ブラウザが自動的に開きます。開かない場合は以下のURLにアクセスしてください:
echo   http://localhost:8501
echo.
echo 終了するにはこのウィンドウを閉じてください。
echo.

"%PYTHON_EXE%" -m streamlit run "%~dp0app.py" --server.headless true --browser.gatherUsageStats false
