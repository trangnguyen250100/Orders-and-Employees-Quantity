-- Thống kê đơn hàng quá deadline

CREATE TABLE test.false_packages STORED AS PARQUET AS 
WITH t1 AS 
(
    SELECT  p.date, p.post_office_id, p.package_id, import_time, operating_time,
        CASE 
            WHEN d.date =  CAST(FROM_timestamp(p.import_time, 'yyyyMMdd') AS INT) 
                AND (CAST(FROM_timestamp(p.import_time, 'HHmm') AS INT) BETWEEN d.start_time AND d.end_time) then p.deadline
            ELSE MINUTES_ADD(HOURS_ADD(TRUNC(d.date,'DD'), d.deadline_hour), d.deadline_minute)
        END deadline
    FROM test.packages AS p
    LEFT JOIN test.deadline AS d 
            ON p.post_office_id = d.post_office_id 
            AND 
            (
                -- lấy mốc deadline đầu tiên
                (
                    (CAST(FROM_timestamp(p.import_time, 'HHmm') AS INT) BETWEEN d.start_time AND d.end_time)
                    AND 
                    (d.date = CAST(FROM_timestamp(p.import_time, 'yyyyMMdd') AS INT))
                )
                OR
                -- lấy các mốc deadline tiếp theo cho đến khi đơn được xử lý
                (  
                    (
                        (
                            (CAST(FROM_timestamp(p.import_time, 'HHmm') AS INT) < d.end_time)
                            AND 
                            (d.date = CAST(FROM_timestamp(p.import_time, 'yyyyMMdd') AS INT))
                        ) 
                        OR  
                        (
                            d.date > CAST(FROM_timestamp(p.import_time, 'yyyyMMdd') AS INT)
                        )
                    )
                    AND 
                    MINUTES_ADD(HOURS_ADD(TRUNC(d.date,'DD'), d.deadline_hour), d.deadline_minute) < operating_time
                )
            )
),
t2 AS
(
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY date, post_office_id, package_id, import_time, deadline_time ORDER BY start_time) AS rn 
    FROM t1 
),
final AS
(
    SELECT 
        date, 
        post_office_id, 
        package_id, 
        import_time, 
        operating_time, 
        deadline_time,
        ROW_NUMBER() OVER (PARTITION BY date, post_office_id, package_id, import_time, operating_time ORDER BY deadline_time) AS deadline_order -- sắp xếp deadline theo thứ tự tăng dần có đánh số thứ tự
    FROM t2 
    WHERE rn = 1 -- loại bỏ dữ liệu trùng lặp
)

SELECT * 
FROM final; 




-- Gán người bị phạt với những đơn quá hạn

CREATE TABLE test.employees_miss_deadlines STORED AS PARQUET AS 
SELECT p.*, 
    em.employee_id
    CASE
        WHEN p.deadline_time is null then false
        WHEN deadline_order = 1 then 
            CASE 
                WHEN em.operating_time_before_deadline is not null AND em.operating_time_after_deadline is not null then true
                WHEN em.operating_time_after_deadline is null AND em.operating_time_before_deadline BETWEEN import_time AND p.deadline_time then true
            END 
        WHEN deadline_order > 1 AND em.operating_time_before_deadline is not null then true
    END is_penalized -- true là bị thẻ, false là không bị thẻ
FROM test.false_packages AS p 
LEFT JOIN test.employees AS em
        ON p.post_office_id = em.post_office_id 
        AND p.deadline_time >= em.previous_deadline AND p.deadline_time < em.next_deadline
        AND em.work_date = p.date
;
