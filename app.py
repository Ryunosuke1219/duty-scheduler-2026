#!/usr/bin/env python3
"""
当直シフトスケジューラ Web UI
Streamlitを使用したWebアプリケーション
"""

import re
import tempfile
import contextlib
import io
from pathlib import Path

import streamlit as st

# scheduler.pyと同じディレクトリにあることを前提
from scheduler import DutyScheduler


def detect_year_month(filename: str) -> tuple[int, int]:
    """ファイル名から年月を自動検出

    パターン例:
    - 2026_3月_shift_schedule.csv
    - 2026_03_shift_schedule.csv
    - 202603_shift_schedule.csv
    """
    # パターン1: 2026_3月 or 2026_03
    m = re.search(r'(\d{4})[_\-](\d{1,2})月?', filename)
    if m:
        return int(m.group(1)), int(m.group(2))

    # パターン2: 202603
    m = re.search(r'(\d{4})(\d{2})', filename)
    if m:
        return int(m.group(1)), int(m.group(2))

    return None, None


def try_read_csv(file_content: bytes) -> tuple[str, str]:
    """CSVファイルを読み込み、エンコーディングを自動判定

    Returns:
        (decoded_content, encoding): デコードされた内容とエンコーディング名
    """
    encodings = ['cp932', 'shift_jis', 'utf-8', 'utf-8-sig']

    for enc in encodings:
        try:
            content = file_content.decode(enc)
            return content, enc
        except (UnicodeDecodeError, LookupError):
            continue

    raise ValueError("ファイルのエンコーディングを判定できませんでした")


def main():
    st.set_page_config(
        page_title="当直シフトスケジューラ",
        page_icon="📅",
        layout="wide"
    )

    st.title("当直シフトスケジューラ")

    # サイドバー
    with st.sidebar:
        st.header("設定")

        # ファイルアップロード
        uploaded_file = st.file_uploader(
            "CSVファイルをアップロード",
            type=['csv'],
            help="勤務可否データを含むCSVファイル（CP932/Shift_JIS/UTF-8対応）"
        )

        # 年月入力
        if uploaded_file is not None:
            detected_year, detected_month = detect_year_month(uploaded_file.name)
            default_year = detected_year if detected_year else 2026
            default_month = detected_month if detected_month else 1
        else:
            default_year = 2026
            default_month = 1

        col1, col2 = st.columns(2)
        with col1:
            year = st.number_input("年", min_value=2020, max_value=2100, value=default_year)
        with col2:
            month = st.number_input("月", min_value=1, max_value=12, value=default_month)

        # 最適化制限時間
        time_limit = st.slider(
            "最適化制限時間（秒）",
            min_value=10,
            max_value=300,
            value=60,
            step=10,
            help="最適化にかける最大時間。長いほど良い解が見つかる可能性があります。"
        )

        # 実行ボタン
        run_button = st.button(
            "当直表を作成する",
            type="primary",
            disabled=uploaded_file is None,
            use_container_width=True
        )

    # メインエリア
    if uploaded_file is None:
        st.info("👈 サイドバーからCSVファイルをアップロードしてください")

        with st.expander("使い方", expanded=True):
            st.markdown("""
            ### 使い方

            1. **CSVファイルを準備**
               - 各医師の勤務可否データを含むCSVファイル
               - 形式: `Name`, `Group`, 日付列（`1(月)`, `2(火)` など）
               - 値: `0`=勤務可能, `1`=勤務不可, `3`=事前割当（当直）, `4`=事前割当（OC）

            2. **ファイルをアップロード**
               - サイドバーの「CSVファイルをアップロード」からファイルを選択
               - ファイル名から年月が自動検出されます

            3. **設定を確認**
               - 年月が正しいか確認（必要に応じて手動で変更）
               - 最適化制限時間を調整（デフォルト: 60秒）

            4. **当直表を作成**
               - 「当直表を作成する」ボタンをクリック
               - 最適化が完了するまで待機

            5. **結果をダウンロード**
               - Excel（スケジュール＋サマリー）
               - CSV（アノテーション付き元データ）
               - カレンダー形式Excel
            """)
        return

    # 実行処理
    if run_button:
        with st.spinner("最適化を実行中..."):
            # 一時ファイルに書き出し
            try:
                content, _ = try_read_csv(uploaded_file.getvalue())
            except ValueError as e:
                st.error(f"エラー: {e}")
                return

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='cp932') as f:
                f.write(content)
                temp_path = f.name

            try:
                # ログをキャプチャ
                log_output = io.StringIO()

                with contextlib.redirect_stdout(log_output):
                    scheduler = DutyScheduler(temp_path, year, month)
                    result = scheduler.solve(time_limit=time_limit)

                if result is None:
                    st.error("解が見つかりませんでした。制約を緩和してください。")
                    with st.expander("実行ログ"):
                        st.code(log_output.getvalue())
                    return

                # 結果を生成
                with contextlib.redirect_stdout(log_output):
                    output = scheduler.generate_output_bytes(result)

                # session_stateに保存
                st.session_state['result'] = result
                st.session_state['output'] = output
                st.session_state['log'] = log_output.getvalue()
                st.session_state['year'] = year
                st.session_state['month'] = month

            except Exception as e:
                st.error(f"エラーが発生しました: {e}")
                with st.expander("実行ログ"):
                    st.code(log_output.getvalue())
                return
            finally:
                # 一時ファイルを削除
                Path(temp_path).unlink(missing_ok=True)

    # 結果表示
    if 'output' in st.session_state:
        output = st.session_state['output']
        year_result = st.session_state['year']
        month_result = st.session_state['month']

        st.success("当直表の作成が完了しました！")

        # 実行ログ
        with st.expander("実行ログ"):
            st.code(st.session_state['log'])

        # サマリーテーブル
        st.subheader("割り当て結果サマリー")
        st.dataframe(
            output['summary_df'],
            use_container_width=True,
            hide_index=True
        )

        # ダウンロードボタン
        st.subheader("ファイルダウンロード")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.download_button(
                label="📊 Excel（スケジュール）",
                data=output['excel'],
                file_name=f"{year_result}_{month_result}月_schedule.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        with col2:
            st.download_button(
                label="📝 CSV（アノテーション付き）",
                data=output['csv'],
                file_name=f"{year_result}_{month_result}月_schedule_annotated.csv",
                mime="text/csv",
                use_container_width=True
            )

        with col3:
            st.download_button(
                label="📅 カレンダー形式Excel",
                data=output['calendar'],
                file_name=f"{year_result}_{month_result}月_schedule_calendar.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )


if __name__ == "__main__":
    main()
