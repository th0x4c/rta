module TPCCHelper
  TPCC_HOME = File.dirname(__FILE__) + '/../'

  MAXITEMS = 100000
  CUST_PER_DIST = 3000
  DIST_PER_WARE = 10
  ORD_PER_DIST = 3000

  CNUM = 1

  def random_number(min, max)
    return min + rand(max - min + 1)
  end

  def lastname(num)
    name = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]
    return name[num / 100] + name[(num / 10) % 10] + name[num % 10]
  end

  def nurand(a, x, y)
    return ((((random_number(0, a) | random_number(x, y)) + CNUM) % (y - x + 1)) + x)
  end
end
