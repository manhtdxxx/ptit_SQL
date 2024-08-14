
-- CÂU 2
---- sub 1: tìm tổng số tiền của 1 KH trên 1 máy ATM
---- sub 2: tìm số tiền max trên 1 máy ATM
----     3: cho số tiền max trên 1 máy ATM = tổng số tiền của 1 KH trên 1 máy ATM để lấy tên KH
select B.max_1atm,B.MaATM,B.MaKH
from (
	select max(sum_1nguoi_1atm) over(partition by A.MaATM) max_1atm, A.sum_1nguoi_1atm, A.MaATM, A.MaKH
	from (
		select sum(SoTienTTHD) sum_1nguoi_1atm, THANHTOANHD.MaATM, KHACHHANG.MaKH
		from THANHTOANHD
		left join THE on THANHTOANHD.MaThe = THE.MaThe
		left join TAIKHOAN on THE.MaTK = TAIKHOAN. MaTK
		left join KHACHHANG on TAIKHOAN.MaKH = KHACHHANG.MaKH
		group by THANHTOANHD.MaATM, KHACHHANG.MaKH
		) as A
	) as B
where sum_1nguoi_1atm = max_1atm


-- CÂU 3:  
with A as (
select top(5) A.MaThe, count(A.MaThe) so_lan_gd
from (
	select THE.MaThe from THE,VAY
	where THE.MaThe= VAY.MaThe
	union all
	select THE.MaThe from THE,THANHTOANHD
	where THE.MaThe= THANHTOANHD.MaThe
	union all
	select THE.MaThe from THE, RUT_GUI
	where THE.MaThe = RUT_GUI.MaThe
	union all
	select THE.MaThe from THE,CHUYENKHOAN
	where THE.MaThe= CHUYENKHOAN.MaThe
	) as A
group by A.MaThe
order by count(A.MaThe) desc
)

select A.*, KHACHHANG.MaKH 
from A
left join THE on THE.MaThe = A.MaThe
left join TAIKHOAN on TAIKHOAN.MaTK = THE.MaTK
left join KHACHHANG on KHACHHANG.MaKH = TAIKHOAN.MaKH

with E as(
select A.MaThe, count(A.MaThe) so_lan_gd
from (
	select THE.MaThe from THE,VAY
	where THE.MaThe= VAY.MaThe
	union all
	select THE.MaThe from THE,THANHTOANHD
	where THE.MaThe= THANHTOANHD.MaThe
	union all
	select THE.MaThe from THE, RUT_GUI
	where THE.MaThe = RUT_GUI.MaThe
	union all
	select THE.MaThe from THE,CHUYENKHOAN
	where THE.MaThe= CHUYENKHOAN.MaThe
	) as A
group by A.MaThe
)
--
select Q.* from
(select E.*, KHACHHANG.MaKH, dense_rank() over(order by so_lan_gd desc) ranked
from E
left join THE on THE.MaThe = E.MaThe
left join TAIKHOAN on TAIKHOAN.MaTK = THE.MaTK
left join KHACHHANG on KHACHHANG.MaKH = TAIKHOAN.MaKH
) as Q
where ranked < 6

-- CÂU 1: nghiệp vụ giao dịch nhiều nhất trong 1 tháng
--- cách 1
create view B as
(select count(*) so_lan, 'TTHD' LOAI
from THANHTOANHD
WHERE YEAR(THANHTOANHD.TimeTTHD) = 2024 AND MONTH(THANHTOANHD.TimeTTHD)= 1
	union all
select count(*) so_lan, 'VAY' LOAI
from VAY
WHERE YEAR(VAY.TimeVay) = 2024 AND MONTH(VAY.TimeVay)= 1
	union all
select count(*) so_lan, 'RG' LOAI
from RUT_GUI
WHERE YEAR(RUT_GUI.TimeRG) = 2024 AND MONTH(RUT_GUI.TimeRG)= 1
	union all
select count(*) so_lan, 'CK' LOAI
from CHUYENKHOAN
WHERE YEAR(CHUYENKHOAN.TimeCK) = 2024 AND MONTH(CHUYENKHOAN.TimeCK)= 1)

select so_lan, LOAI
from B
order by so_lan desc


--- cách 2
with C as (
select count(*) so_lan, datepart(year,TimeTTHD) y, datepart(month,TimeTTHD) m, 'TTHD' LOAI
from THANHTOANHD
group by datepart(year,TimeTTHD), datepart(month,TimeTTHD)
	union all
select count(*) so_lan, datepart(year,TimeVay) y, datepart(month,TimeVay) m, 'VAY' LOAI
from VAY
group by datepart(year,TimeVay), datepart(month,TimeVay)
	union all
select count(*) so_lan, datepart(year,TimeRG) y, datepart(month,TimeRG) m, 'RG' LOAI
from RUT_GUI
group by datepart(year,TimeRG), datepart(month,TimeRG)
	union all
select count(*) so_lan, datepart(year,TimeCK) y, datepart(month,TimeCK) m, 'CK' LOAI
from CHUYENKHOAN
group by datepart(year,TimeCK), datepart(month,TimeCK)
)

select so_lan, y, m, LOAI, dense_rank() over(order by so_lan desc) ranked
from C
where y = 2025 and m = 6
order by so_lan desc
---