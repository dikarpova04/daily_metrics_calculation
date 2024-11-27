import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import psycopg2

def get_data_from_db(query, params, host, user, password):
    conn = psycopg2.connect(
        dbname='courses'
        , user=user
        , password=password 
        , target_session_attrs='read-write'
        , host=host
        , port='5432'

    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            data_df = pd.DataFrame(data, columns=columns)
        return data_df

    except Exception as e:
        print(f"Error while fetching data: {e}")

    finally:
        conn.close()


def fetch_payments_data(current_date, host, user, password):
    query = """
        SELECT *
        FROM python_for_da.central_cinema_user_payments
        WHERE payment_date BETWEEN %s AND %s
    """
    start_date = current_date - relativedelta(months=2)
    end_date = current_date + timedelta(days=1)

    payments_data = get_data_from_db(query, (start_date, end_date), host, user, password)

    payments_data['payment_date'] = pd.to_datetime(payments_data['payment_date'])
    payments_data['payment_day'] = payments_data['payment_date'].dt.to_period('D').dt.to_timestamp()

    return payments_data

def fetch_partner_commission_data(host, user, password):
    query = """
        SELECT partner_commission_id, partner_id, commission, valid_from, valid_to
        FROM python_for_da.central_cinema_partner_commission
    """
    partner_comission = get_data_from_db(query, (), host, user, password)

    return partner_comission

def fetch_user_activity_data(current_date, host, user, password):

    query = """
        SELECT user_activity_id, user_id, title_id, play_start, play_end
        FROM python_for_da.central_cinema_user_activity
        WHERE play_start BETWEEN %s AND %s
    """

    start_date = current_date - relativedelta(months=2)
    end_date = current_date + timedelta(days=1)

    user_activity = get_data_from_db(query, (start_date, end_date), host, user, password)

    user_activity['play_start'] = pd.to_datetime(user_activity['play_start'])
    user_activity['play_end'] = pd.to_datetime(user_activity['play_end'])

    user_activity['session_duration_minutes'] = (user_activity['play_end'] - user_activity[
        'play_start']).dt.total_seconds() / 60

    return user_activity


def fetch_title_data(host, user, password):
    query = """
        SELECT title_id, title_name, release_date, duration, genres, popularity
        FROM python_for_da.central_cinema_title
    """

    title_data = get_data_from_db(query, (), host, user, password)

    return title_data


def safe_divide(numerator, denominator):
    return round((numerator / denominator) * 100, 2) if denominator != 0 else 0


def calculate_payments_metrics(payments_data, yesterday, yesterday_last_month, payment_type='trial'):

    is_trial_flag = 1 if payment_type == 'trial' else 0
    metric_name = "Количество триалов" if payment_type == 'trial' else "Количество оплат"

    payments_yesterday = payments_data[
        (payments_data['is_trial'] == is_trial_flag) &
        (payments_data['payment_day'] == yesterday)
        ]['user_payment_id'].count()

    payments_yesterday_last_month = payments_data[
        (payments_data['is_trial'] == is_trial_flag) &
        (payments_data['payment_day'] == yesterday_last_month)
        ]['user_payment_id'].count()

    payments_delta = safe_divide(payments_yesterday, payments_yesterday_last_month) 

    symbol_payments_delta = "\U0001F4C8" + "\U00002705" if payments_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f'''
    {metric_name}:
    Вчера: {payments_yesterday}
    Прошлый месяц: {payments_yesterday_last_month}
    МоМ %: {symbol_payments_delta} {payments_delta}%
    '''

    return message


def calculate_conversion_to_first_payment(payments_data, yesterday, yesterday_last_month, yesterday_prev_month):

    # конверсия текущего месяца
    users_trial_last_month = payments_data[
        (payments_data['is_trial'] == 1) &
        (payments_data['payment_day'] == yesterday_last_month)
        ]['user_id'].unique()

    users_non_trial_yesterday_filtered = payments_data[
        (payments_data['is_trial'] == 0) &
        (payments_data['payment_day'] == yesterday) &
        (payments_data['user_id'].isin(users_trial_last_month))
        ]['user_id'].nunique()

    conv_last_month = safe_divide(users_non_trial_yesterday_filtered, len(users_trial_last_month))

    # конверсия прошлого месяца
    users_trial_prev_month = payments_data[
        (payments_data['is_trial'] == 1) &
        (payments_data['payment_day'] == yesterday_prev_month)
        ]['user_id'].unique()

    users_non_trial_yesterday_prev_filtered = payments_data[
        (payments_data['is_trial'] == 0) &
        (payments_data['payment_day'] == yesterday_last_month)
        & (payments_data['user_id'].isin(users_trial_prev_month))
        ]['user_id'].nunique()

    conv_prev_month = safe_divide(users_non_trial_yesterday_prev_filtered, len(users_trial_prev_month))

    conv_delta = safe_divide(conv_last_month, conv_prev_month)

    symbol_conv_delta = "\U0001F4C8" + "\U00002705" if conv_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f'''
    Конверсия в первую оплату
    Вчера: {conv_last_month}%
    Прошлый месяц: {conv_prev_month}%
    МоМ %: {symbol_conv_delta} {conv_delta}%
    '''

    return message


def calculate_gross_cash_in(payments_data, yesterday, yesterday_last_month):
    ARPU = 299

    # сумма оплат с признаком is_trial = 0 за вчера
    cash_in_yesterday = payments_data[
                            (payments_data['is_trial'] == 0) &
                            (payments_data['payment_day'] == yesterday)
                            ]['user_id'].count() * ARPU

    # сумма оплат с признаком is_trial = 0 за аналогичный день прошлого месяца
    cash_in_last_month = payments_data[
                             (payments_data['is_trial'] == 0) &
                             (payments_data['payment_day'] == yesterday_last_month)
                             ]['user_id'].count() * ARPU

    cash_in_delta = safe_divide(cash_in_yesterday, cash_in_last_month)

    symbol_cash_in_delta = "\U0001F4C8" + "\U00002705" if cash_in_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f'''
    Валовый cash-in
    Вчера: {cash_in_yesterday}
    Прошлый месяц: {cash_in_last_month}
    МоМ %: {symbol_cash_in_delta} {cash_in_delta}%
    '''

    return message


def calculate_cac(payments_data, partner_comission, yesterday, yesterday_last_month):

    #  данные о платежах с данными о комиссиях
    partner_comission_merged = payments_data.merge(partner_comission, how='left', on='partner_id')

    # средняя комиссия для вчера
    avg_commision_yesterday = round(partner_comission_merged[
                                        (partner_comission_merged['is_trial'] == 1) &
                                        (partner_comission_merged['payment_day'] == yesterday)
                                        ]['commission'].mean(), 2)

    # средняя комиссия для аналогичного дня прошлого месяца
    avg_commision_last_month = round(partner_comission_merged[
                                         (partner_comission_merged['is_trial'] == 1) &
                                         (partner_comission_merged['payment_day'] == yesterday_last_month)
                                         ]['commission'].mean(), 2)

    avg_commision_delta = safe_divide(avg_commision_yesterday, avg_commision_last_month)

    symbol_avg_commision_delta = "\U0001F4C8" + "\U00002705" if avg_commision_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f'''
    CAC
    Вчера: {avg_commision_yesterday}
    Прошлый месяц: {avg_commision_last_month}
    МоМ %: {symbol_avg_commision_delta} {avg_commision_delta}%
    '''

    return message


def calculate_avg_session_duration(user_activity_data, yesterday, yesterday_last_month):

    avg_session_duration_yesterday = round(user_activity_data[
                                               user_activity_data['play_start'].dt.date == yesterday.date()
                                               ]['session_duration_minutes'].mean(), 2)

    avg_session_duration_last_month = round(user_activity_data[
                                                user_activity_data['play_start'].dt.date == yesterday_last_month.date()
                                                ]['session_duration_minutes'].mean(), 2)

    avg_session_duration_delta = safe_divide(avg_session_duration_yesterday, avg_session_duration_last_month)

    symbol_avg_session_duration_delta = "\U0001F4C8" + "\U00002705" if avg_session_duration_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f'''
    Среднее время сессии
    Вчера: {avg_session_duration_yesterday} минут
    Прошлый месяц: {avg_session_duration_last_month} минут
    МоМ %: {symbol_avg_session_duration_delta} {avg_session_duration_delta}%
    '''

    return message


def calculate_completion_rate(user_activity_data, title_data, yesterday, yesterday_last_month):

    merged_data = user_activity_data.merge(title_data, on='title_id', how='left')

    merged_data['completion_rate'] = merged_data['session_duration_minutes'] / merged_data['duration']

    avg_completion_yesterday = round(merged_data[
                                         merged_data['play_start'].dt.date == yesterday.date()
                                         ]['completion_rate'].mean(), 2)

    avg_completion_last_month = round(merged_data[
                                          merged_data['play_start'].dt.date == yesterday_last_month.date()
                                          ]['completion_rate'].mean(), 2)

    avg_completion_delta = safe_divide(avg_completion_yesterday, avg_completion_last_month)

    symbol_avg_completion_delta = "\U0001F4C8" + "\U00002705" if avg_completion_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f'''
    Досматриваемость
    Вчера: {avg_completion_yesterday}
    Прошлый месяц: {avg_completion_last_month}
    МоМ %: {symbol_avg_completion_delta} {avg_completion_delta}%
    '''

    return message


def calculate_unique_viewers(user_activity_data, yesterday, yesterday_last_month):

    unique_viewers_yesterday = user_activity_data[
        user_activity_data['play_start'].dt.date == yesterday.date()
        ]['user_id'].nunique()

    unique_viewers_last_month = user_activity_data[
        user_activity_data['play_start'].dt.date == yesterday_last_month.date()
        ]['user_id'].nunique()

    unique_viewers_delta = safe_divide(unique_viewers_yesterday, unique_viewers_last_month)
    
    symbol_unique_viewers_delta = "\U0001F4C8" + "\U00002705" if unique_viewers_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f'''
    Количество уникальных смотревших
    Вчера: {unique_viewers_yesterday}
    Прошлый месяц: {unique_viewers_last_month}
    МоМ %: {symbol_unique_viewers_delta} {unique_viewers_delta}%
    '''

    return message

# НОВЫЕ МЕТРИКИ
def repeat_viewers_per_week(user_activity, 
    yesterday, 
    yesterday_last_month, 
    days_back=7,
    min_session_duration=30):
    """
    Рассчитывает и сравнивает количество активных повторных зрителей за указанный период и аналогичный период месяц назад.

    :param user_activity: DataFrame с данными активности пользователей
    :param yesterday: вчерашняя дата
    :param yesterday_last_month: дата месяц назад
    :param days_back: количество дней для анализа (по умолчанию неделя)
    :param min_session_duration: минимальная продолжительность сеанса (в минутах, по умолчанию 30)
    :return: строка с результатами для включения в отчет
    """

    def calculate_active_viewers(activity_data, reference_date, days, min_session_duration):
        """
        Вспомогательная функция для расчета количества активных зрителей за указанный период.
        """
        start_date = reference_date - timedelta(days=days)

        recent_activity = activity_data[
            (activity_data['play_start'].dt.date >= start_date.date()) &
            (activity_data['play_start'].dt.date <= reference_date.date()) &
            (user_activity['session_duration_minutes'] >= min_session_duration)
        ]

        user_view_counts = recent_activity.groupby('user_id').size()
        # считаем юзеров, которые просматривали контент более 2 раз за неделю
        return user_view_counts[user_view_counts > 2].count()  

    active_viewers_recent = calculate_active_viewers(user_activity, yesterday, days_back, min_session_duration)
    active_viewers_last_month = calculate_active_viewers(user_activity, yesterday_last_month, days_back, min_session_duration)

    active_viewers_delta = round((active_viewers_recent / active_viewers_last_month) * 100, 2)
    symbol_active_viewers_delta = "\U0001F4C8" + "\U00002705" if active_viewers_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f"""
    Active Repeat Viewers (за последние {days_back} дней):
    Вчера и последние {days_back} дней: {active_viewers_recent}
    Прошлый месяц (аналогичный период): {active_viewers_last_month}
    МоМ %: {symbol_active_viewers_delta} {active_viewers_delta}%
    """
    return message

def calculate_average_sessions_per_viewer(user_activity, 
                                          yesterday, 
                                          yesterday_last_month,
                                          days_back=7,
                                          min_session_duration=45):
    """
    Рассчитывает среднее количество сессий на пользователя за последние N дней.
    
    :param user_activity: DataFrame с данными активности пользователей
    :param yesterday: вчерашняя дата
    :param last_month_date: аналогичная дата месяц назад
    :param days_back: количество дней для анализа (по умолчанию 7)
    :param min_session_duration: минимальная продолжительность сессии в минутах (по умолчанию 45)
    :return: итоговое сообщение
    """
    
    def calculate_average_sessions(user_activity, end_date, days_back, min_session_duration):
        start_date = end_date - timedelta(days=days_back)
        
        recent_activity = user_activity[
            (user_activity['play_start'].dt.date >= start_date.date()) &
            (user_activity['play_start'].dt.date <= end_date.date()) &
            (user_activity['session_duration_minutes'] >= min_session_duration)
        ]
        
        total_sessions = recent_activity.shape[0]
        unique_viewers = recent_activity['user_id'].nunique()
        
        # среднее количество сессий на пользователя
        average_sessions = total_sessions / unique_viewers if unique_viewers > 0 else 0
        return average_sessions

    # за последние N дней
    asp_recent = calculate_average_sessions(user_activity, yesterday, days_back, min_session_duration)
    # за аналогичный период месяц назад
    asp_last_month = calculate_average_sessions(user_activity, yesterday_last_month, days_back, min_session_duration)

    asp_delta = round((asp_recent / asp_last_month) * 100, 2)
    symbol_asp_delta = "\U0001F4C8" + "\U00002705" if asp_delta > 100 else "\U0001F4C8 \U0001F53B"

    message = f"""
    Average Sessions Per Viewer (7 days):
    Вчера и последние {days_back} дней: {asp_recent:.2f}
    Прошлый месяц (аналогичный период): {asp_last_month:.2f}
    МоМ %: {symbol_asp_delta} {asp_delta}%
    """
    
    return message


def calculate_all_metrics(today, host, user, password):
    yesterday = datetime.strptime((today-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')
    yesterday_last_month = datetime.strptime((today-relativedelta(months = 1)-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')
    yesterday_prev_month = datetime.strptime((today-relativedelta(months = 2)-timedelta(days = 1)).strftime('%Y-%m-%d'),'%Y-%m-%d')

    payments_data = fetch_payments_data(today, host, user, password)
    partner_comission = fetch_partner_commission_data(host, user, password)
    user_activity_data = fetch_user_activity_data(today, host, user, password)
    title_data = fetch_title_data(host, user, password)

    trial_message = calculate_payments_metrics(payments_data, yesterday, yesterday_last_month, payment_type='trial')
    all_payments_message = calculate_payments_metrics(payments_data, yesterday, yesterday_last_month, payment_type='all')
    conversion_message = calculate_conversion_to_first_payment(payments_data, yesterday, yesterday_last_month, yesterday_prev_month)
    cash_in_message = calculate_gross_cash_in(payments_data, yesterday, yesterday_last_month)
    cac_message = calculate_cac(payments_data, partner_comission, yesterday, yesterday_last_month)
    session_duration_message = calculate_avg_session_duration(user_activity_data, yesterday, yesterday_last_month)
    completion_rate_message = calculate_completion_rate(user_activity_data, title_data, yesterday, yesterday_last_month)
    unique_viewers_message = calculate_unique_viewers(user_activity_data, yesterday, yesterday_last_month)
    # НОВЫЕ МЕТРИКИ
    repeat_viewers_per_week_message = repeat_viewers_per_week(user_activity_data, yesterday, yesterday_last_month)
    average_sessions_per_viewer_per_week_message = calculate_average_sessions_per_viewer(user_activity_data, yesterday, yesterday_last_month)

    final_message = "\n".join([
        trial_message,
        all_payments_message,
        conversion_message,
        cash_in_message,
        cac_message,
        session_duration_message,
        completion_rate_message,
        unique_viewers_message,
        repeat_viewers_per_week_message,
        average_sessions_per_viewer_per_week_message
    ])

    return final_message
