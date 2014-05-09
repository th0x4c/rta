# -*- coding: utf-8 -*-
require 'yaml'
require 'thread'
import java.sql.DriverManager
import Java::oracle.jdbc.pool.OracleDataSource

class String
  def undent
    gsub(/^.{#{(slice(/^ +/) || '').length}}/, '')
  end
end

class TPCH < RTA::Session
  TPCH_HOME = File.dirname(__FILE__)
  REFRESH_COUNT_FILE = TPCH_HOME + '/config/refresh_count'

  @@start_throughput_test = false

  def setup
    # ログ
    self.log = RTA::Log.new(TPCH_HOME + "/log/tpch_#{Time.now.strftime("%Y%m%d%H%M%S")}_#{self.session_id}.log")
    # self.log.level = RTA::Log::DEBUG

    config = Hash.new
    File.open(TPCH_HOME + '/config/config.yml', 'r') do |file|
      config = YAML.load(file.read)
    end

    @refresh_count = File.open(REFRESH_COUNT_FILE).read.to_i

    # 接続
    begin
      @ds = OracleDataSource.new
      @ds.setURL(config["tpch_url"])
      @ds.setUser(config["tpch_user"])
      @ds.setPassword(config["tpch_password"])
      @con = @ds.getConnection
      @con.setAutoCommit(false)
    rescue SQLException => e
      e.printStackTrace
    rescue ClassNotFoundException => e
      e.printStackTrace
    end

    # load transaction
    tx_load = RTA::Transaction.new("tpch load") do
      create_directory(@con, config["os_directory"])
      create_external_table(@con, config["parallel_degree"])
      create_table(@con, config["parallel_degree"], config["table_tablespace"])
      create_index(@con, config["parallel_degree"], config["index_tablespace"])
      analyze(@con, config["parallel_degree"], config["tpch_user"])
    end

    # power test RF1 transaction
    tx_power_rf1 = RTA::Transaction.new("tpch power test RF1") do
      @refresh_count += 1
      refresh_function_1(@con, @refresh_count, config["parallel_degree"])
    end

    # power test transaction
    power_query_txs = query_file_to_transactions(TPCH_HOME + "/query/query.0", @con)
    power_query_txs.each do |tx|
      tx.stat.name = tx.stat.name.sub(/tpch/, "tpch power test")
    end

    # power test RF2 transaction
    tx_power_rf2 = RTA::Transaction.new("tpch power test RF2") do
      refresh_function_2(@con, @refresh_count, config["parallel_degree"])
      File.open(REFRESH_COUNT_FILE, 'w') do |file|
        file.puts @refresh_count
      end
    end
    tx_power_rf2.after { @@start_throughput_test = true }

    # throughput test transaction
    throughput_query_txs = query_file_to_transactions(TPCH_HOME + "/query/query.#{self.session_id}", @con)

    # throughput test RF1, RF2 transaction
    throughput_refresh_txs = Array.new
    (1..self.sessions.size).each do |sid|
      rf1 = RTA::Transaction.new("tpch throughput test RF1 (#{sid})") do
        @refresh_count += 1
        refresh_function_1(@con, @refresh_count, config["parallel_degree"])
      end

      rf2 = RTA::Transaction.new("tpch throughput test RF2 (#{sid})") do
        refresh_function_2(@con, @refresh_count, config["parallel_degree"])
        File.open(REFRESH_COUNT_FILE, 'w') do |file|
          file.puts @refresh_count
        end
      end

      throughput_refresh_txs << rf1
      throughput_refresh_txs << rf2
    end

    @tx_last = RTA::Transaction.new("last") do
      @con.commit
    end

    if self.session_id == 1
      @txs = [tx_power_rf1] + power_query_txs + [tx_power_rf2] +
             throughput_query_txs + throughput_refresh_txs + [@tx_last]
      @txs.unshift(tx_load) if @refresh_count == 0
    else
      @txs = throughput_query_txs + [@tx_last]
    end

    @txs_idx = 0

    self.go
  end

  def transaction
    tx = @txs[@txs_idx]

    if @txs_idx == 0 && tx.name =~ /tpch Q\d+/
      sleep 0.01 until @@start_throughput_test
    end

    if tx.name =~ /tpch throughput test RF1 \(\d+\)/
      self.sessions.each do |ses|
        while ses.transactions
                 .reverse
                 .find { |t| t.name =~ /tpch Q\d+/ }
                 .count == 0
          sleep 0.01
        end
      end
    end

    self.stop if tx.name == "last"
    @txs_idx += 1
    self.log.info("Start #{tx.name}")
    puts Time.now.strftime("%Y-%m-%d %X") + " sid: #{self.session_id}, msg: \"Start #{tx.name}\""

    return tx
  end

  def transactions
    return @txs
  end

  def teardown
    if self.session_id == 1
      self.log.info(executive_summary)
      puts executive_summary
    end
  end

  private
  def create_directory(con, path)
    sql = "CREATE DIRECTORY rta_tpch_dir AS '#{path}'"
    exec_sql(con, sql)
  end

  def create_external_table(con, parallel_degree)
    sqls = Array.new

    # l_shipdate, l_commitdate and l_receiptdate are DATE type with MASK
    # "YYYY-MM-DD" -> CHAR(10). TO_DATE-ed when loading.
    sql = <<-EOS
      CREATE TABLE lineitem_et
      (
        l_orderkey      NUMBER,
        l_partkey       NUMBER,
        l_suppkey       NUMBER,
        l_linenumber    NUMBER,
        l_quantity      NUMBER,
        l_extendedprice NUMBER,
        l_discount      NUMBER,
        l_tax           NUMBER,
        l_returnflag    CHAR(1),
        l_linestatus    CHAR(1),
        l_shipdate      CHAR(10),
        l_commitdate    CHAR(10),
        l_receiptdate   CHAR(10),
        l_shipinstruct  CHAR(25),
        l_shipmode      CHAR(10),
        l_comment       VARCHAR(44)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'lineitem', parallel_degree)

    # o_orderdate is DATE type with MASK "YYYY-MM-DD" -> CHAR(10).
    # TO_DATE-ed when loading.
    sql = <<-EOS
      CREATE TABLE orders_et
      (
        o_orderkey      NUMBER,
        o_custkey       NUMBER,
        o_orderstatus   CHAR(1),
        o_totalprice    NUMBER,
        o_orderdate     CHAR(10),
        o_orderpriority CHAR(15),
        o_clerk         CHAR(15),
        o_shippriority  NUMBER,
        o_comment       VARCHAR(79)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'orders', parallel_degree)

    sql = <<-EOS
      CREATE TABLE partsupp_et
      (
        ps_partkey    NUMBER,
        ps_suppkey    NUMBER,
        ps_availqty   NUMBER,
        ps_supplycost NUMBER,
        ps_comment    VARCHAR(199)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'partsupp', parallel_degree)

    sql = <<-EOS
      CREATE TABLE part_et
      (
        p_partkey     NUMBER,
        p_name        VARCHAR(55),
        p_mfgr        CHAR(25),
        p_brand       CHAR(10),
        p_type        VARCHAR(25),
        p_size        NUMBER,
        p_container   CHAR(10),
        p_retailprice NUMBER,
        p_comment     VARCHAR(23)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'part', parallel_degree)

    sql = <<-EOS
      CREATE TABLE customer_et
      (
        c_custkey    NUMBER,
        c_name       VARCHAR(25),
        c_address    VARCHAR(40),
        c_nationkey  NUMBER,
        c_phone      CHAR(15),
        c_acctbal    NUMBER,
        c_mktsegment CHAR(10),
        c_comment    VARCHAR(117)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'customer', parallel_degree)

    sql = <<-EOS
      CREATE TABLE supplier_et
      (
        s_suppkey   NUMBER,
        s_name      CHAR(25),
        s_address   VARCHAR(40),
        s_nationkey NUMBER,
        s_phone     CHAR(15),
        s_acctbal   NUMBER,
        s_comment   VARCHAR(101)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'supplier', parallel_degree)

    sql = <<-EOS
      CREATE TABLE nation_et
      (
        n_nationkey NUMBER,
        n_name      CHAR(25),
        n_regionkey NUMBER,
        n_comment   VARCHAR(152)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'nation', 1)

    sql = <<-EOS
      CREATE TABLE region_et
      (
        r_regionkey NUMBER,
        r_name      CHAR(25),
        r_comment   VARCHAR(152)
      )
    EOS
    sqls << add_organization_external_clause(sql, 'region', 1)

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def add_organization_external_clause(sql, table_name, parallel_degree)
    sql += <<-EOS
      ORGANIZATION EXTERNAL
      (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY rta_tpch_dir
        ACCESS PARAMETERS
        (
          RECORDS DELIMITED BY NEWLINE
          BADFILE '#{table_name}.bad'
          LOGFILE '#{table_name}.log'
          FIELDS TERMINATED BY '|'
          MISSING FIELD VALUES ARE NULL
        )
        LOCATION
        (
    EOS

    if parallel_degree > 1
      sql += (1..parallel_degree).map { |n| "          rta_tpch_dir:'#{table_name}.tbl.#{n}'" }
                                 .join(",\n")
    else
      sql += "          rta_tpch_dir:'#{table_name}.tbl'"
    end
    sql += "\n"

    sql += <<-EOS
        )
      )
      REJECT LIMIT UNLIMITED
      PARALLEL #{parallel_degree}
    EOS
  end

  def create_table(con, parallel_degree, tablespace_name)
    sqls = Array.new

    # lineitem
    sql = <<-EOS
      CREATE TABLE lineitem
      (
        l_shipdate      ,
        l_orderkey      NOT NULL,
        l_discount      NOT NULL,
        l_extendedprice NOT NULL,
        l_suppkey       NOT NULL,
        l_quantity      NOT NULL,
        l_returnflag    ,
        l_partkey       NOT NULL,
        l_linestatus    ,
        l_tax           NOT NULL,
        l_commitdate    ,
        l_receiptdate   ,
        l_shipmode      ,
        l_linenumber    NOT NULL,
        l_shipinstruct  ,
        l_comment            
      )
      PARALLEL #{parallel_degree}
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      PARTITION BY RANGE (l_shipdate)
      SUBPARTITION BY HASH (l_partkey)
      SUBPARTITIONS #{parallel_degree}
      (
    EOS

    # from 1992-01-01 to 1998-11-01
    part_dates = (1992..1998).map do |year|
                   (1..12).map do |month|
                     sprintf("%4d-%02d-01", year, month)
                   end
                 end.flatten[0..-2]
    part_dates.each_with_index do |part_date, i|
      sql += "        PARTITION item#{i + 1} VALUES LESS THAN (TO_DATE('#{part_date}', 'YYYY-MM-DD')),\n"
    end

    sql += <<-EOS
        PARTITION item#{part_dates.size + 1} VALUES LESS THAN (MAXVALUE)
      )
      AS SELECT
        TO_DATE(l_shipdate, 'YYYY-MM-DD'),
        l_orderkey,
        l_discount,
        l_extendedprice,
        l_suppkey,
        l_quantity,
        l_returnflag,
        l_partkey,
        l_linestatus,
        l_tax,
        TO_DATE(l_commitdate, 'YYYY-MM-DD'),
        TO_DATE(l_receiptdate, 'YYYY-MM-DD'),
        l_shipmode,
        l_linenumber,
        l_shipinstruct,
        l_comment
      FROM lineitem_et
      ORDER BY l_orderkey
    EOS

    sqls << sql

    # orders
    sql = <<-EOS
      CREATE TABLE orders
      (
        o_orderdate     ,
        o_orderkey      NOT NULL,
        o_custkey       NOT NULL,
        o_orderpriority ,
        o_shippriority  ,
        o_clerk         ,
        o_orderstatus   ,
        o_totalprice    ,
        o_comment       
      ) 
      PARALLEL #{parallel_degree}
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      PARTITION BY RANGE (o_orderdate)
      SUBPARTITION BY HASH (o_custkey)
      SUBPARTITIONS #{parallel_degree}
      (
    EOS

    # from 1992-01-01 to 1998-08-01
    part_dates = (1992..1998).map do |year|
                   (1..12).map do |month|
                     sprintf("%4d-%02d-01", year, month)
                   end
                 end.flatten[0..-5]
    part_dates.each_with_index do |part_date, i|
      sql += "        PARTITION ord#{i + 1} VALUES LESS THAN (TO_DATE('#{part_date}', 'YYYY-MM-DD')),\n"
    end

    sql += <<-EOS
        PARTITION ord#{part_dates.size + 1} VALUES LESS THAN (MAXVALUE)
      )
      AS SELECT
        TO_DATE(o_orderdate, 'YYYY-MM-DD'),
        o_orderkey,
        o_custkey,
        o_orderpriority,
        o_shippriority,
        o_clerk,
        o_orderstatus,
        o_totalprice,
        o_comment
      FROM orders_et
      ORDER BY o_orderkey
    EOS

    sqls << sql

    # partsupp
    sql = <<-EOS
      CREATE TABLE partsupp
      (
        ps_partkey    NOT NULL,
        ps_suppkey    NOT NULL,
        ps_supplycost ,
        ps_availqty   ,
        ps_comment
      )
      PARALLEL #{parallel_degree}
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      PARTITION BY HASH (ps_partkey)
      PARTITIONS #{parallel_degree}
      AS SELECT
        ps_partkey,
        ps_suppkey,
        ps_supplycost,
        ps_availqty,
        ps_comment
      FROM partsupp_et
    EOS
    sqls << sql

    # part
    sql = <<-EOS
      CREATE TABLE part
      (
        p_partkey     NOT NULL,
        p_type        ,
        p_size        ,
        p_brand       ,
        p_name        ,
        p_container   ,
        p_mfgr        ,
        p_retailprice ,
        p_comment
      )
      PARALLEL #{parallel_degree}
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      PARTITION BY HASH (p_partkey)
      PARTITIONS #{parallel_degree}
      AS SELECT
        p_partkey,
        p_type,
        p_size,
        p_brand,
        p_name,
        p_container,
        p_mfgr,
        p_retailprice,
        p_comment
      FROM part_et
    EOS
    sqls << sql

    # customer
    sql = <<-EOS
      CREATE TABLE customer
      (
        c_custkey    NOT NULL,
        c_mktsegment ,
        c_nationkey  ,
        c_name       ,
        c_address    ,
        c_phone      ,
        c_acctbal    ,
        c_comment
      )
      PARALLEL #{parallel_degree}
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      PARTITION BY HASH (c_custkey)
      PARTITIONS #{parallel_degree}
      AS SELECT
        c_custkey,
        c_mktsegment,
        c_nationkey,
        c_name,
        c_address,
        c_phone,
        c_acctbal,
        c_comment
      FROM customer_et
    EOS
    sqls << sql

    # supplier
    sql = <<-EOS
      CREATE TABLE supplier
      (
        s_suppkey   NOT NULL,
        s_nationkey ,
        s_comment   ,
        s_name      ,
        s_address   ,
        s_phone     ,
        s_acctbal
      )
      PARALLEL #{parallel_degree}
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      PARTITION BY HASH (s_suppkey)
      PARTITIONS #{parallel_degree}
      AS SELECT
        s_suppkey,
        s_nationkey,
        s_comment,
        s_name,
        s_address,
        s_phone,
        s_acctbal
      FROM supplier_et
    EOS
    sqls << sql

    # nation
    sql = <<-EOS
      CREATE TABLE nation
      (
        n_nationkey NOT NULL,
        n_name      ,
        n_regionkey ,
        n_comment
      )
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      AS SELECT * FROM nation_et
    EOS
    sqls << sql

    # region
    sql = <<-EOS
      CREATE TABLE region
      (
        r_regionkey ,
        r_name      ,
        r_comment
      )
      NOLOGGING
      COMPRESS
      TABLESPACE #{tablespace_name}
      AS SELECT * FROM region_et
    EOS
    sqls << sql

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def create_index(con, parallel_degree, tablespace_name)
    sqls = Array.new

    sql = <<-EOS
      CREATE INDEX i_l_orderkey
      ON lineitem (l_orderkey)
      GLOBAL PARTITION BY HASH (l_orderkey)
      PARTITIONS #{parallel_degree}
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
      COMPUTE STATISTICS
      NOLOGGING
    EOS
    sqls << sql

    sql = <<-EOS
      CREATE UNIQUE INDEX i_o_orderkey
      ON orders (o_orderkey)
      GLOBAL PARTITION BY HASH (o_orderkey)
      PARTITIONS #{parallel_degree}
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
      COMPUTE STATISTICS
      NOLOGGING
    EOS
    sqls << sql

    sql = <<-EOS
      CREATE UNIQUE INDEX i_c_custkey
      ON customer (c_custkey)
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
      COMPUTE STATISTICS
      NOLOGGING
    EOS
    sqls << sql

    sql = <<-EOS
      CREATE UNIQUE INDEX ps_pkey_skey
      ON partsupp (ps_partkey,ps_suppkey)
      GLOBAL PARTITION BY HASH (ps_partkey)
      PARTITIONS #{parallel_degree}
      TABLESPACE #{tablespace_name}
      PARALLEL #{parallel_degree}
      COMPUTE STATISTICS
      NOLOGGING
    EOS
    sqls << sql

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
  end

  def analyze(con, parallel_degree, schema)
    sql = <<-EOS.chomp.undent
      begin
        DBMS_STATS.GATHER_SCHEMA_STATS(ownname          => '#{schema.upcase}',
                                       estimate_percent => 1,
                                       degree           => #{parallel_degree},
                                       granularity      => 'GLOBAL',
                                       method_opt       => 'for all columns size 1');
      end;
    EOS

    self.log.info(sql_log_str(sql))
    cstmt = nil
    begin
      cstmt = con.prepareCall(sql)
      cstmt.executeUpdate
    ensure
      cstmt.close if cstmt
    end
  end

  def refresh_function_1(con, nth, parallel_degree)
    sqls = Array.new

    # l_shipdate, l_commitdate and l_receiptdate are DATE type with MASK
    # "YYYY-MM-DD" -> CHAR(10). TO_DATE-ed when loading.
    sql = <<-EOS
      CREATE TABLE temp_lineitem_et
      (
        l_orderkey      NUMBER,
        l_partkey       NUMBER,
        l_suppkey       NUMBER,
        l_linenumber    NUMBER,
        l_quantity      NUMBER,
        l_extendedprice NUMBER,
        l_discount      NUMBER,
        l_tax           NUMBER,
        l_returnflag    CHAR(1),
        l_linestatus    CHAR(1),
        l_shipdate      CHAR(10),
        l_commitdate    CHAR(10),
        l_receiptdate   CHAR(10),
        l_shipinstruct  CHAR(25),
        l_shipmode      CHAR(10),
        l_comment       VARCHAR(44)
      )
      ORGANIZATION EXTERNAL
      (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY rta_tpch_dir
        ACCESS PARAMETERS
        (
          RECORDS DELIMITED BY NEWLINE
          NOBADFILE
          NOLOGFILE
          FIELDS TERMINATED BY '|'
          MISSING FIELD VALUES ARE NULL
        )
        LOCATION
        (
          'lineitem.tbl.u#{nth}'
        )
      )
      REJECT LIMIT UNLIMITED
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    # o_orderdate is DATE type with MASK "YYYY-MM-DD" -> CHAR(10).
    # TO_DATE-ed when loading.
    sql = <<-EOS
      CREATE TABLE temp_orders_et
      (
        o_orderkey      NUMBER,
        o_custkey       NUMBER,
        o_orderstatus   CHAR(1),
        o_totalprice    NUMBER,
        o_orderdate     CHAR(10),
        o_orderpriority CHAR(15),
        o_clerk         CHAR(15),
        o_shippriority  NUMBER,
        o_comment       VARCHAR(79)
      )
      ORGANIZATION EXTERNAL
      (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY rta_tpch_dir
        ACCESS PARAMETERS
        (
          RECORDS DELIMITED BY NEWLINE
          NOBADFILE
          NOLOGFILE
          FIELDS TERMINATED BY '|'
          MISSING FIELD VALUES ARE NULL
        )
        LOCATION
        (
          'orders.tbl.u#{nth}'
        )
      )
      REJECT LIMIT UNLIMITED
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    sql = <<-EOS
      INSERT INTO orders
      SELECT
        TO_DATE(o_orderdate, 'YYYY-MM-DD'),
        o_orderkey,
        o_custkey,
        o_orderpriority,
        o_shippriority,
        o_clerk,
        o_orderstatus,
        o_totalprice,
        o_comment
      FROM temp_orders_et
    EOS
    sqls << sql

    sql = <<-EOS
      INSERT INTO lineitem
      SELECT
        TO_DATE(l_shipdate, 'YYYY-MM-DD'),
        l_orderkey,
        l_discount,
        l_extendedprice,
        l_suppkey,
        l_quantity,
        l_returnflag,
        l_partkey,
        l_linestatus,
        l_tax,
        TO_DATE(l_commitdate, 'YYYY-MM-DD'),
        TO_DATE(l_receiptdate, 'YYYY-MM-DD'),
        l_shipmode,
        l_linenumber,
        l_shipinstruct,
        l_comment
      FROM temp_lineitem_et
    EOS
    sqls << sql

    sqls << "DROP TABLE temp_lineitem_et"
    sqls << "DROP TABLE temp_orders_et"

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
    con.commit
  end

  def refresh_function_2(con, nth, parallel_degree)
    sqls = Array.new

    sql = <<-EOS
      CREATE TABLE temp_orderkey_et
      (
        t_orderkey      NUMBER
      )
      ORGANIZATION EXTERNAL
      (
        TYPE ORACLE_LOADER
        DEFAULT DIRECTORY rta_tpch_dir
        ACCESS PARAMETERS
        (
          RECORDS DELIMITED BY NEWLINE
          NOBADFILE
          NOLOGFILE
          FIELDS TERMINATED BY '|'
          MISSING FIELD VALUES ARE NULL
        )
        LOCATION
        (
          'delete.#{nth}'
        )
      )
      REJECT LIMIT UNLIMITED
      PARALLEL #{parallel_degree}
    EOS
    sqls << sql

    sql = <<-EOS
      CREATE TABLE temp_orderkey
      PARALLEL #{parallel_degree}
      NOLOGGING
      AS SELECT * FROM temp_orderkey_et
    EOS
    sqls << sql

    sql = <<-EOS
      CREATE UNIQUE INDEX i_temp_orderkey
      ON temp_orderkey (t_orderkey)
      PARALLEL #{parallel_degree}
      NOLOGGING
      COMPUTE STATISTICS
    EOS
    sqls << sql

    sql = <<-EOS
      DELETE FROM
      (
        SELECT o.rowid
        FROM orders o, temp_orderkey t
        WHERE o.o_orderkey = t.t_orderkey
        ORDER BY 1
      )
    EOS
    sqls << sql

    sql = <<-EOS
      DELETE FROM
      (
        SELECT l.rowid
        FROM lineitem l, temp_orderkey t
        WHERE l.l_orderkey = t.t_orderkey
        ORDER BY 1
      )
    EOS
    sqls << sql

    sqls << "DROP TABLE temp_orderkey"
    sqls << "PURGE TABLE temp_orderkey"
    sqls << "DROP TABLE temp_orderkey_et"

    sqls.each { |sql| exec_sql(con, sql.chomp.undent) }
    con.commit
  end

  def exec_sql(con, sql)
    self.log.info(sql_log_str(sql))
    stmt = nil
    begin
      stmt = con.createStatement
      stmt.executeUpdate(sql)
    ensure
      stmt.close if stmt
    end
  end

  def sql_log_str(sql)
    return ("SQL> " + sql + "\n/").each_line.map { |x| "     " + x }.join.lstrip
  end

  def parse_query_file(query_file)
    queries = Array.new
    queries_str = File.open(query_file).read

    seed = $1.to_i if queries_str =~ /-- using (\d+) as a seed to the RNG/

    while (query_str, rownum, queries_str = queries_str.partition(/where rownum <= (-?\d+);\n/))[0] != ""
      rownum = $1.to_i
      comment, _, sql_str = query_str.partition(/\n\r?\n\r?/)
      qid = $1.to_i if comment =~ /\(Q(\d+)\)/
      sql = sql_str.strip.chomp(';').delete("\r")

      if rownum > 0
        sql = "select * from (\n" + sql + "\n) where rownum <= #{rownum}"
      end

      if qid == 15
        sql = query_variant_Q15(sql)
      end

      if [7, 8, 9, 13, 22].include?(qid)
        sql = avoid_ora933(sql, qid)
      end

      queries << { :qid     => qid,
                   :seed    => seed,
                   :comment => comment.strip.delete("\r"),
                   :rownum  => rownum,
                   :sql     => sql                         }
    end

    return queries
  end

  def avoid_ora933(sql, qid)
    case qid
    when 7
      return sql.sub(/as shipping/, "shipping")
    when 8
      return sql.sub(/as all_nations/, "all_nations")
    when 9
      return sql.sub(/as profit/, "profit")
    when 13
      return sql.sub(/count\(o_orderkey\)/, "count(o_orderkey) c_count")
                .sub(/as c_orders \(c_custkey, c_count\)/, "c_orders")
    when 22
      return sql.sub(/as custsale/, "custsale")
                .gsub(/substring\(c_phone from 1 for 2\)/, "substr(c_phone, 1, 2)")
    end
  end

  def query_variant_Q15(sql)
    # Appendix B: APPROVED QUERY VARIANTS
    # Q15
    # Common table expressions can be thought of as shared table expressions
    # or "inline views" that last only for the duration of the query.
    return sql.gsub(/revenue\d+/, 'revenue')
              .sub(/create view revenue \(supplier_no, total_revenue\) as\r?\n/,
                   "with revenue (supplier_no, total_revenue) as (\n")
              .sub(/;(\r?\n)+/, "\n)\n")
              .sub(/;(\r?\n)+drop view revenue/, '')
              .strip
  end

  def query_file_to_transactions(query_file, con)
    queries = parse_query_file(query_file)

    txs = Array.new
    queries.each do |query|
      txs << RTA::Transaction.new("tpch Q#{query[:qid]}") do
        self.log.info(query[:comment])

        exec_query(con, query[:sql])
      end
    end

    return txs
  end

  def exec_query(con, sql)
    self.log.info(sql_log_str(sql))
    stmt = nil
    begin
      stmt = con.createStatement
      rset = stmt.executeQuery(sql)
      rsmd = rset.getMetaData
      cc = rsmd.getColumnCount
      
      # print column name
      self.log.info((1..cc).map { |n| rsmd.getColumnLabel(n) }.join(", "))

      rownum = 0
      while rset.next
        self.log.info((1..cc).map { |n| rset.getObject(n).to_s }.join(", "))
        rownum += 1
      end

      self.log.info("#{rownum} row(s) selected.")
      rset.close
    ensure
      stmt.close if stmt
    end
  end

  def executive_summary
    es = Array.new
    es << "============================================================================================"
    es << "==================================== Executive Summary ====================================="
    es << "============================================================================================"

    tx_load = self.sessions[0].transactions.find { |tx| tx.name == "tpch load" }

    es << "+---------------------+-----------------------------------------+--------------------------+"
    es << "|    Database Size    |     Composite Query per Hour Metric     |    Database Load Time    |"
    es << "+---------------------+-----------------------------------------+--------------------------+"
    es << "|" + centering("#{scale_factor} GB", 21) + "|" +
          centering("#{sprintf("%.2f", tpch_composite)} QphH@#{scale_factor}GB", 41) + "|" +
          centering(tx_load ? format_sec(tx_load.total_elapsed_time) : "-", 26) + "|"
    es << "+---------------------+-----------------------------------------+--------------------------+"
    es << ""

    es << graphic_representation
    es << ""

    es << "+------------------------------------------------------------------------------------------+"
    es << "Measurement Results"
    es << sprintf("         Database Scaling (SF/Size) %56d", scale_factor)
    es << sprintf("         Start of Database Load Time %55s", tx_load ? tx_load.first_time : "-")
    es << sprintf("         End of Database Load Time %57s", tx_load ? tx_load.end_time : "-")
    es << sprintf("         Database Load Time %64s", tx_load ? tx_load.total_elapsed_time : "-")

    es << sprintf("         Query Streams for Throughput Test (S) %45d", self.sessions.size)

    es << sprintf("         TPC-H Power %71.2f", tpch_power)
    es << sprintf("         TPC-H Throughput %66.2f", tpch_throughput)
    es << sprintf("         TPC-H Composite %67.2f", tpch_composite)
    es << ""

    es << "Measurement Interval"
    es << sprintf("         Measurement Interval in Throughput Test (Ts) %38d",
                  measurement_interval_in_throughput_test)
    es << ""

    es << "Duration of stream execution:"
    es << "+----------+----------+-------------------+--------+-------------------+-------------------+"
    es << "|          |   Seed   | Query Start Time  |Duration|  RF1 Start Time   |  RF2 Start Time   |"
    es << "|  Power   |          |  Query End Time   | (sec)  |   RF1 End Time    |   RF2 End Time    |"
    es << "|   Run    +----------+-------------------+--------+-------------------+-------------------+"

    query_txs = self.sessions[0]
                    .transactions
                    .find_all { |tx| tx.name =~ /tpch power test Q\d+/ }
    query_start_time = query_txs.map { |tx| tx.first_time }.min
    query_end_time = query_txs.map { |tx| tx.end_time }.max
    duration = query_end_time - query_start_time

    rf1 = self.sessions[0]
              .transactions
              .find { |tx| tx.name == "tpch power test RF1" }
    rf2 = self.sessions[0]
              .transactions
              .find { |tx| tx.name == "tpch power test RF2" }

    es << "|          |" + sprintf("%10d|", seed(0)) +
                         query_start_time.strftime("%Y-%m-%d %X") + "|" +
                         sprintf("%7d |", duration) +
                         rf1.first_time.strftime("%Y-%m-%d %X") + "|" +
                         rf2.first_time.strftime("%Y-%m-%d %X") + "|"
    es << "|          |" + "          |" +
                         query_end_time.strftime("%Y-%m-%d %X") + "|" +
                         "        |" +
                         rf1.end_time.strftime("%Y-%m-%d %X") + "|" +
                         rf2.end_time.strftime("%Y-%m-%d %X") + "|"
    es << "+----------+----------+-------------------+--------+-------------------+-------------------+"
    es << ""
    es << "+----------+----------+-------------------+--------+-------------------+-------------------+"
    es << "|Throughput|   Seed   | Query Start Time  |Duration|  RF1 Start Time   |  RF2 Start Time   |"
    es << "|  Stream  |          |  Query End Time   | (sec)  |   RF1 End Time    |   RF2 End Time    |"
    es << "+----------+----------+-------------------+--------+-------------------+-------------------+"

    (1..self.sessions.size).each do |stream|
      query_txs = self.sessions[stream - 1]
                      .transactions
                      .find_all { |tx| tx.name =~ /tpch Q\d+/ }
      query_start_time = query_txs.map { |tx| tx.first_time }.min
      query_end_time = query_txs.map { |tx| tx.end_time }.max
      duration = query_end_time - query_start_time

      rf1 = self.sessions[0]
                .transactions
                .find { |tx| tx.name == "tpch throughput test RF1 (#{stream})" }
      rf2 = self.sessions[0]
                .transactions
                .find { |tx| tx.name == "tpch throughput test RF2 (#{stream})" }

      es << sprintf("| %8d |", stream) +
            sprintf("%10d|", seed(stream)) +
            query_start_time.strftime("%Y-%m-%d %X") + "|" +
            sprintf("%7d |", duration) +
            rf1.first_time.strftime("%Y-%m-%d %X") + "|" +
                         rf2.first_time.strftime("%Y-%m-%d %X") + "|"
      es << "|          |" + "          |" +
            query_end_time.strftime("%Y-%m-%d %X") + "|" +
            "        |" +
            rf1.end_time.strftime("%Y-%m-%d %X") + "|" +
            rf2.end_time.strftime("%Y-%m-%d %X") + "|"
      es << "+----------+----------+-------------------+--------+-------------------+-------------------+"
    end
    es << ""

    es << "                            TPC-H Timing Intervals (in seconds)                              "
    es << ""
    es << "Duration of query execution:"
    es << "+------+------+------+------+------+------+------+------+------+------+------+------+------+"
    es << "|Stream|  Q1  |  Q2  |  Q3  |  Q4  |  Q5  |  Q6  |  Q7  |  Q8  |  Q9  |  Q10 |  Q11 |  Q12 |"
    es << "|  ID  |      |      |      |      |      |      |      |      |      |      |      |      |"
    es << "+------+------+------+------+------+------+------+------+------+------+------+------+------+"

    q = timing_intervals_for_query
    rf = timing_intervals_for_refresh_function

    (0..self.sessions.size).each do |stream|
      es << sprintf("|%5d |", stream) + (1..12).inject("") { |x, i| x + sprintf("%6.1f|", q[i][stream]) }
    end
    es << "|  Min |" + (1..12).inject("") { |x, i| x + sprintf("%6.1f|", q[i].min) }
    es << "|  Max |" + (1..12).inject("") { |x, i| x + sprintf("%6.1f|", q[i].max) }
    es << "|  Avg |" + (1..12).inject("") { |x, i| x + sprintf("%6.1f|", avg(q[i])) }
    es << "+------+------+------+------+------+------+------+------+------+------+------+------+------+"
    es << ""

    es << "+------+------+------+------+------+------+------+------+------+------+------+------+------+"
    es << "|Stream|  Q13 |  Q14 |  Q15 |  Q16 |  Q17 |  Q18 |  Q19 |  Q20 |  Q21 |  Q22 |  RF1 |  RF2 |"
    es << "|  ID  |      |      |      |      |      |      |      |      |      |      |      |      |"
    es << "+------+------+------+------+------+------+------+------+------+------+------+------+------+"

    (0..self.sessions.size).each do |stream|
      es << sprintf("|%5d |", stream) +
              (13..22).inject("") { |x, i| x + sprintf("%6.1f|", q[i][stream]) } +
              sprintf("%6.1f|", rf[1][stream]) +
              sprintf("%6.1f|", rf[2][stream])
    end
    es << "|  Min |" +
            (13..22).inject("") { |x, i| x + sprintf("%6.1f|", q[i].min) } +
            sprintf("%6.1f|", rf[1].min) +
            sprintf("%6.1f|", rf[2].min)
    es << "|  Max |" +
            (13..22).inject("") { |x, i| x + sprintf("%6.1f|", q[i].max) } +
            sprintf("%6.1f|", rf[1].max) +
            sprintf("%6.1f|", rf[2].max)
    es << "|  Avg |" +
            (13..22).inject("") { |x, i| x + sprintf("%6.1f|", avg(q[i])) } +
            sprintf("%6.1f|", avg(rf[1])) +
            sprintf("%6.1f|", avg(rf[2]))
    es << "+------+------+------+------+------+------+------+------+------+------+------+------+------+"

    return es.join("\n")
  end

  def graphic_representation
    gr = Array.new

    gr << "                               Query Times in seconds      ooo Power Run"
    gr << "                                                           *** Throughput Run"
    gr << ""

    q = timing_intervals_for_query
    rf = timing_intervals_for_refresh_function

    max = ( [ rf[1][0],
              rf[2][0],
              avg(rf[1].drop(1)),
              avg(rf[2].drop(1))  ] +
            (1..22).map { |qid| q[qid][0] } +
            (1..22).map { |qid| avg(q[qid].drop(1)) } ).max

    max_graph = max * 1.2

    2.downto(1) do |rfid|
      percent_str = "               RF#{rfid} |"
      percent = rf[rfid][0] * 100 / max_graph
      # 100.times { |i| percent_str << (i < percent ? "o" : " ") }
      50.times { |i| percent_str << (i*2 < percent ? "o" : " ") }
      percent_str << sprintf(" %6.1f", rf[rfid][0])
      gr << percent_str

      percent_str = "                   |"
      percent = avg(rf[rfid].drop(1)) * 100 / max_graph
      # 100.times { |i| percent_str << (i < percent ? "*" : " ") }
      50.times { |i| percent_str << (i*2 < percent ? "*" : " ") }
      percent_str << sprintf(" %6.1f", avg(rf[rfid].drop(1)))
      gr << percent_str
    end

    22.downto(1) do |qid|
      percent_str = "               " + sprintf("%3s", "Q#{qid}") + " |"
      percent = q[qid][0] * 100 / max_graph
      # 100.times { |i| percent_str << (i < percent ? "o" : " ") }
      50.times { |i| percent_str << (i*2 < percent ? "o" : " ") }
      percent_str << sprintf(" %6.1f", q[qid][0])
      gr << percent_str

      percent_str = "                   |"
      percent = avg(q[qid].drop(1)) * 100 / max_graph
      # 100.times { |i| percent_str << (i < percent ? "*" : " ") }
      50.times { |i| percent_str << (i*2 < percent ? "*" : " ") }
      percent_str << sprintf(" %6.1f", avg(q[qid].drop(1)))
      gr << percent_str
    end

    gr << "                   +------------------------+------------------------+"
    half = sprintf("%.1f", max_graph / 2)
    gr << "                   0                        " +
         half + (" " * (25 - half.size)) +
         sprintf("%.1f", max_graph)

    return gr.join("\n")
  end

  def timing_intervals_for_query
    q = Array.new
    (1..22).each { |n| q[n] = Array.new }

    (1..22).each do |n|
      q[n][0] = self.sessions[0]
                    .transactions
                    .find { |tx| tx.name == "tpch power test Q#{n}" }
                    .total_elapsed_time

      (1..self.sessions.size).each do |stream|
        q[n][stream] =  self.sessions[stream - 1]
                            .transactions
                            .find { |tx| tx.name == "tpch Q#{n}" }
                            .total_elapsed_time
      end
    end

    return q
  end

  def timing_intervals_for_refresh_function
    rf = Array.new
    rf[1] = Array.new
    rf[2] = Array.new

    rf[1][0] = self.sessions[0]
                   .transactions
                   .find { |tx| tx.name == "tpch power test RF1" }
                   .total_elapsed_time

    rf[2][0] = self.sessions[0]
                   .transactions
                   .find { |tx| tx.name == "tpch power test RF2" }
                   .total_elapsed_time

    (1..self.sessions.size).each do |stream|
      rf[1][stream] =  self.sessions[0]
                           .transactions
                           .find { |tx| tx.name == "tpch throughput test RF1 (#{stream})" }
                           .total_elapsed_time

      rf[2][stream] =  self.sessions[0]
                           .transactions
                           .find { |tx| tx.name == "tpch throughput test RF1 (#{stream})" }
                           .total_elapsed_time
    end

    return rf
  end

  def centering(str, size)
    left = " " * ((size - str.size) / 2)
    right = " " * (size - str.size - left.size)
    return left + str + right
  end

  def format_sec(sec)
    hour = sec.to_i / 3600
    minute = sec.to_i / 60 % 60
    second = sec % 60

    return sprintf("%02d:%02d:%.3f", hour, minute, second)
  end

  def avg(ary)
    return ary.inject { |sum, i| sum + i }.to_f / ary.size
  end

  def seed(stream)
    return parse_query_file(TPCH_HOME + "/query/query.#{stream}")[0][:seed]
  end

  def scale_factor
    sf = nil
    File.open(TPCH_HOME + '/config/config.sh') do |file|
        file.each_line { |line| sf = $1.to_i if line =~ /SCALE_FACTOR=(\d+)/ }
    end
    return sf
  end

  def tpch_power
    query_intervals = self.sessions[0]
                          .transactions
                          .find_all { |tx| tx.name =~ /tpch power test/ }
                          .map { |ptx| ptx.total_elapsed_time }

    return 3600 * Math.exp(-1.0/24.0 * (query_intervals.inject(0) { |x, i| x + Math.log(i) })) * scale_factor
  end

  def tpch_throughput
    streams = self.sessions.size

    return (streams * 22 * 3600).to_f / measurement_interval_in_throughput_test * scale_factor
  end

  def tpch_composite
    return Math.sqrt(tpch_power * tpch_throughput)
  end

  def measurement_interval_in_throughput_test
    all_throughput_test_txs = Array.new
    self.sessions.each do |ses|
      all_throughput_test_txs += ses.transactions
                                    .find_all { |tx| tx.name =~ /tpch Q\d+/ || tx.name =~ /tpch throughput test/ }
    end

    first_time = all_throughput_test_txs.map { |tx| tx.first_time }.min
    end_time = all_throughput_test_txs.map { |tx| tx.end_time }.max
    return end_time - first_time
  end
end
