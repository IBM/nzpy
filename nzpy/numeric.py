import ctypes

NDIGIT_INT64 = False
MAX_NUMERIC_DIGIT_COUNT = 4
NUMERIC_MAX_PRECISION = 38
HI32_MASK = 0xffffffff00000000
USE_MUL_DOUBLE = False
const_data_ten = []


class NumericVar():

    def __init__(self):
        
         self.data           = []
         self.scale          = None        
         self.rscale         = None
         self.rprecision     = None	

def int32_to_uint32(i):
    return ctypes.c_uint32(i).value
    
def base() :
	return (1 << 32)

def highPart(val):
	return (val >> 32)

def lowPart(val):
	return (val & (1 << 32) - 1)

def encodeNum(words, low, hi) :
	words[0] = int(lowPart(low))
	words[1] = int(highPart(low))
	words[2] = int(lowPart(hi))
	words[3] = int(highPart(hi))

def decodeNum(words, low, hi) :
	low = words[0] | words[1]*(1<<32)
	hi = words[2] | words[3]*(1<<32)

def isNumeric_Data_Negative(numdataP):
	return (numdataP[0] & 0x80000000) != 0

def copy_128(srcP):
    destP = []
    destP.append(srcP[0])
    destP.append(srcP[1])
    destP.append(srcP[2])
    destP.append(srcP[3])
    return destP

def negate_128(arg):
    #First complement the value (1's complement)
    for i in range(MAX_NUMERIC_DIGIT_COUNT) :
        arg[i] = int32_to_uint32(~arg[i])
	
	#Then increment it to form 2's complement (negative)
    return inc_128(arg)

#for 2's complement
def inc_128(arg ):
	i = MAX_NUMERIC_DIGIT_COUNT
	carry = True
	bInputNegative = isNumeric_Data_Negative(arg)

	while (i != 0) and carry :
		i -= 1
		work = (arg[i]) + 1
		carry = (work & HI32_MASK) != 0
		arg[i] = work & 0xffffffff

	if not bInputNegative :
		return isNumeric_Data_Negative(arg)
	else:
		return False
	
def div_128(numeratorP, denominatorP, resultP):

    hiquotient = 0 
    loquotient = 0
    hiremainder = 0
    loremainder = 0
    bResNegative = isNumeric_Data_Negative(numeratorP) != isNumeric_Data_Negative(denominatorP)
    
    num = copy_128(numeratorP)
    if isNumeric_Data_Negative(num) :
        if negate_128(num) :
            return True
            
    den = copy_128(denominatorP)
    if isNumeric_Data_Negative(den):
        if negate_128(den):
            return True
            
    hinum = ((num[0]) << 32) + (num[1])
    lonum = ((num[2]) << 32) + (num[3])
    hidenom = ((den[0]) << 32) + (den[1])
    lodenom = ((den[2]) << 32) + (den[3])
    
    if div_and_round_double(1, lonum, hinum, lodenom, hidenom, loquotient, hiquotient, loremainder, hiremainder) != 0 :
        return True
    
    resultP[0] = (((hiquotient) & HI32_MASK) >> 32)
    resultP[1] = (hiquotient & 0xffffffff)
    resultP[2] = (((loquotient) & HI32_MASK) >> 32)
    resultP[3] = (loquotient & 0xffffffff)
    
    if bResNegative :
        if negate_128(resultP):
            return True			
	
    return False

def div10_128(numeratorP, quotientP):
	
    remainder = 0
    
    for i in range(MAX_NUMERIC_DIGIT_COUNT) :
        work = numeratorP[i] + (remainder<<32)
        if work != 0 :
            quotientP[i] = int(work / 10)
            remainder = int(work % 10)
        else :
            quotientP[i] = 0
            remainder = 0
    return remainder

def PYTHON_numeric_load_var(dataP, precision, scale, digitCount):
	
    varP = NumericVar()
    
    #extend sign
    sign = dataP[0] & 0x80000000
    if sign != 0 :
        leadDigit = 0xffffffff
    else:
        leadDigit = 0
    
    i = 0
    while i < MAX_NUMERIC_DIGIT_COUNT-digitCount:
        varP.data.append(leadDigit)
        i +=1
        
    j = 0
    while i < MAX_NUMERIC_DIGIT_COUNT :
        varP.data.append(dataP[j])
        j +=1
        i +=1
    
    varP.scale = scale
    varP.rscale = scale
    varP.rprecision = precision

    return varP

# ----------
#  round_var() -
#       Rounds a numeric var to a target scale.
#  overflow will return true.
# ----------

def round_var(nvar, scale) :

    for i in range(MAX_NUMERIC_DIGIT_COUNT):
        const_data_ten.append(0)
		
    const_data_ten[MAX_NUMERIC_DIGIT_COUNT-1] = 10
    
    aDD = scale - nvar.scale # additional decimal digits
    positive = not isNumeric_Data_Negative(nvar.data)
    temp = []
    
    if nvar.scale == scale :
        return False
        
    workData = copy_128(nvar.data)
    if not positive :
        if negate_128(workData) :
            return True
            
    if aDD < 0 :
        if (aDD != -1) and div_128(workData, power_of_10(-aDD-1), workData) :
            return True
        
        round = (div10_128(workData, temp) > 4) #for rounding ro next number
        if div_128(workData, const_data_ten, workData):
            return True
            
        if round :
            if inc_128(workData) :
                return True
                
    elif aDD > 0 :
        if mul_128(workData, power_of_10(aDD), workData) :
            return True
            
    nvar.scale = scale
    nvar.rscale = scale # !FIX-jpb   rounding should change result scale, right?
    nvar.data = copy_128(workData)
    if not positive :
        if negate_128(nvar.data) :
            return True
            
    return False
    
# Multiply two doubleword integers with doubleword result.
# Return nonzero if the operation overflows, assuming it's signed.
# Each argument is given as two `int64' pieces.
# One argument is L1 and H1; the other, L2 and H2.
# The value is stored as two `int64' pieces in *LV and *HV.
def mul_double(l1, h1, l2, h2, lv, hv):
    
    arg1 = []
    arg2 = []
    prod = [0, 0, 0, 0, 0, 0, 0, 0]	
    toplow = 0
    tophigh = 0
    neglow = 0
    neghigh = 0
    
    encodeNum(arg1, l1, h1)
    encodeNum(arg2, l2, h2)
    
    for i in range(4):
        carry = 0
        for j in range(4) :
            k = i + j
			# This product is <= 0xFFFE0001, the sum <= 0xFFFF0000.
            carry += arg1[i] * arg2[j]
			# Since prod[p] < 0xFFFF, this sum <= 0xFFFFFFFF. 
            carry += prod[k]
            prod[k] = lowPart(carry)
            carry = highPart(carry)
            
        prod[i+4] = carry
        
    decodeNum(prod[:4], lv, hv) # This ignores prod[4] through prod[4*2-1] 

	# Check for overflow by calculating the top half of the answer in full;
	# it should agree with the low half's sign bit.
    decodeNum(prod[4:], toplow, tophigh)
    if h1 < 0 :
        neg_double(l2, h2, neglow, neghigh)
        add_double(neglow, neghigh, toplow, tophigh, toplow, tophigh)
    
    if h2 < 0 :
        neg_double(l1, h1, neglow, neghigh)
        add_double(neglow, neghigh, toplow, tophigh, toplow, tophigh)
        
    if hv < 0 :
        return ((~(toplow & tophigh)) != 0)
    else :
        return ((toplow | tophigh) != 0)

# Negate a doubleword integer with doubleword result.
# Return nonzero if the operation overflows, assuming it's signed.
# The argument is given as two `int64' pieces in L1 and H1.
# The value is stored as two `int64' pieces in *LV and *HV.
def neg_double(l1 , h1, lv, hv):
	if l1 == 0 :
		lv = 0
		hv = -h1
		if (hv & h1) < 0 : 
			return 1
		else :
			return 0		
	else:
		lv = -l1
		hv = ~h1
		return 0    

def overflow_sum_sign(a, b, sum) :
	if (~(a ^ b) & (a ^ sum)) < 0 :
		return 1
	else :
		return 0
        
# Add two doubleword integers with doubleword result.
# Each argument is given as two `int64' pieces.
# One argument is L1 and H1; the other, L2 and H2.
# The value is stored as two `int64' pieces in *LV and *HV.
def add_double(l1, h1, l2, h2, lv, hv):
    l = l1 + l2
    
    if l < l1 :
        badd = 1
    else :
        badd = 0
        
    h = h1 + h2 + badd
    
    lv = l
    hv = h
    
    return overflow_sum_sign(h1, h2, h)

#Get Numeric variable value represented in string format
def get_str_from_var(nvar, dscale):
    
    res = ""
    unbiasedDigits = []
    work = []
    bLeadingZeroes = True
    pos = 0
    
    bNegative = isNumeric_Data_Negative(nvar.data)
    
    if round_var(nvar, dscale) :
        return ""
        
    workData = copy_128(nvar.data)
    
    if bNegative :
        if negate_128(workData) :
            return ""
            
    for tmp in range(NUMERIC_MAX_PRECISION) :
        unbiasedDigits.append(div10_128(workData, workData))
        
    unbiasedDigits = unbiasedDigits[::-1]
    
    for tmp in range(NUMERIC_MAX_PRECISION) :
		# suppress leading zeros, but force output of a digit before implied decimal point
        if (tmp < NUMERIC_MAX_PRECISION-dscale-1) and bLeadingZeroes and (unbiasedDigits[tmp] == 0) :
            continue
        bLeadingZeroes = False
        work.append((unbiasedDigits[tmp] + 0))
        pos += 1
        
    work.append(0) # terminate sork string
    
    tmp = pos # strlen of work data
    
    if bNegative :
        res += '-'
        
    if dscale != 0 :
        iplaces = tmp - dscale #value before decimal
        for i in range(iplaces) :
            res += str(work[i])
            
        res += '.' #decimal point
        pos +=1
        for i in range(dscale) :    #1 more size to copy \0
            res += str(work[i+iplaces])
        
    else :
        for i in range(tmp) :
            res += str(work[i])
            
    dstSpace = res
    return dstSpace    

def mul_128(v1, v2, vRes):
	# We treat the arguments as having 8 16-bit digits and do long multiplications
	# as in the days of the 3 Rs
	#
	# Here's an example with 3 digit numbers written ABC and DEF.  C and F are in the
	# units' (i.e. "base to the zeroth power) position, B and E in the "base" position,
	# and A and D in the "base squared" position.
	#
	# The units position of the product will be the low digit of C*F.
	# The "base" position of the product will be the low digit of (B*F+C*E) plus
	# the carry digit from the first step (this is the high digit of the product C*F.
	# The "base squared" position of the product will be the low digit of (A*F+B*E+C*D)
	# plus carry.  The "base cubed" position of the product will be the low digit of
	# (A*E+B*D) plus carry. The "base to the fourth" position of the product will be
	# the low digit of A*D plus carry.  And the "base to the fifth" position of the
	# product will be the carry.
	#
	# We load the 4 32-bit digit value of "v1" into the 8 16-bit digit value "a",
	# load "v1" into "b", compute the 32-bit sums of products, as in the example
	# above, and store them into the 15 32-bit digit "work" value.  Finally, we
	# step through the "work" value entries, adding any carry, and assigning the low
	# 16-bit digit of each entry to the corresponding 16-bit digit of the 16 16-bit
	# digit result "c"
	#
	# !FIX-jpb this should be optimized
    
    if USE_MUL_DOUBLE :
        lv = hv = 0
        h1 = (v1[0] << 32) + v1[1]
        l1 = (v1[2] << 32) + v1[3]
        h2 = (v2[0] << 32) + v2[1]
        l2 = (v2[2] << 32) + v2[3]
        bRetVal = mul_double(l1, h1, l2, h2, lv, hv)
        vRes[0] = int((hv & HI32_MASK) >> 32)
        vRes[1] = int(hv & 0xffffffff)
        vRes[2] = int((lv & HI32_MASK) >> 32)
        vRes[3] = int(lv & 0xffffffff)
        return bRetVal
        
    else :
        carry = 0
        
        if (v1[0] | v1[1] | v1[2] | v2[0] | v2[1] | v2[2]) == 0 :
            val = v1[3] * v2[3]
            vRes[3] = int(val & 0xFFFFFFFF)
            vRes[2] = int(val >> 32)
            vRes[1] = 0
            vRes[0] = 0
            return False
            
        bResNegative = (isNumeric_Data_Negative(v1) != isNumeric_Data_Negative(v2))
        v1abs = copy_128(v1)
        if isNumeric_Data_Negative(v1abs) :
            if negate_128(v1abs) :
                return True
                
        v2abs = copy_128(v2)
        if isNumeric_Data_Negative(v2abs) :
            if negate_128(v2abs) :
                return True
        
        a = load_8_digit(v1abs)
        b = load_8_digit(v2abs)
        
        w.append(a[0]*b[0])
        w.append(a[1]*b[0] + a[0]*b[1])
        w.append(a[2]*b[0] + a[1]*b[1] + a[0]*b[2])
        w.append(a[3]*b[0] + a[2]*b[1] + a[1]*b[2] + a[0]*b[3])
        w.append(a[4]*b[0] + a[3]*b[1] + a[2]*b[2] + a[1]*b[3] + a[0]*b[4])
        w.append(a[5]*b[0] + a[4]*b[1] + a[3]*b[2] + a[2]*b[3] + a[1]*b[4] + a[0]*b[5])
        w.append(a[6]*b[0] + a[5]*b[1] + a[4]*b[2] + a[3]*b[3] + a[2]*b[4] + a[1]*b[5] + a[0]*b[6])
        w.append(a[7]*b[0] + a[6]*b[1] + a[5]*b[2] + a[4]*b[3] + a[3]*b[4] + a[2]*b[5] + a[1]*b[6] + a[0]*b[7])
        w.append(a[7]*b[1] + a[6]*b[2] + a[5]*b[3] + a[4]*b[4] + a[3]*b[5] + a[2]*b[6] + a[1]*b[7])
        w.append(a[7]*b[2] + a[6]*b[3] + a[5]*b[4] + a[4]*b[5] + a[3]*b[6] + a[2]*b[7])
        w.append(a[7]*b[3] + a[6]*b[4] + a[5]*b[5] + a[4]*b[6] + a[3]*b[7])
        w.append(a[7]*b[4] + a[6]*b[5] + a[5]*b[6] + a[4]*b[7])
        w.append(a[7]*b[5] + a[6]*b[6] + a[5]*b[7])
        w.append(a[7]*b[6] + a[6]*b[7])
        w.append(a[7] * b[7])
        
        i = 15
        while i > 0 : 
            w[i-1] += carry
            c.append(int(w[i-1] & 0xffff))
            carry = (int(w[i-1] >> 16))
            i -=1
            
        c.append(carry) # hi order digit is final carry
        c = c[::-1] # reverse list
        bRetVal = store_8_digit_from_16(c[:], vRes)
        
        if bResNegative :
            if negate_128(vRes) :
                return True
                
    return bRetVal


def load_8_digit(src):
    dest = []
    for i in range(MAX_NUMERIC_DIGIT_COUNT) :
        dest.append(src[i] >> 16)
        dest.append(src[i] & 0xffffffff)
        
    return dest 
    
# tests the hi order 8 digits of src[] for overflow and stores the low order 8 in dest
def store_8_digit_from_16(src, dest) :
	    
	for i in range(2*MAX_NUMERIC_DIGIT_COUNT):
		if src[i] != 0 :
			return True # overflow
		
	for j in range(MAX_NUMERIC_DIGIT_COUNT): 
		dest[j] = int((src[i] << 16) + src[i+1])
		i += 2
	
	return False   

# mul10_and_add multiplies non-negative TNumericData in place by 10 and adds an int
def mul10_and_add(data, adder) :
    
    i = MAX_NUMERIC_DIGIT_COUNT - 1
    carry = adder
    
    while i >= 0 : 
        work = data[i]*10 + carry
        data[i] = int(work & 0xffffffff)
        carry = (work >> 32)
        i -= 1
        
    return (carry != 0) # true=> overflow

def power_of_10(exponent) :
    
    powersOfTen = []
    next = []
    needsInit = True
    
    if needsInit :
        next.append(0)
        next.append(0)
        next.append(0)
        next.append(1)
        
        for i in range(NUMERIC_MAX_PRECISION) :
            powersOfTen[i].append(next[0])
            powersOfTen[i].append(next[1])
            powersOfTen[i].append(next[2])
            powersOfTen[i].append(next[3])
            if mul10_and_add(next, 0) : # use convenient helper routine in this one-time initing
                #assert(false);          # shouldn't happen if our loop limit correct
                pass 
                
        needsInit = False
        
    if exponent < NUMERIC_MAX_PRECISION :
        return powersOfTen[exponent]
    elif exponent == NUMERIC_MAX_PRECISION :
        return powersOfTen[0]       # NUMERIC_MAX_PRECISIONth needed for get_digit_count, but entry not used
    else:
        return None                 # This will never arise as its made sure scale will limit to MAX_NUMERIC_DIGIT_COUNT	

# Negate a doubleword integer with doubleword result.
# Return nonzero if the operation overflows, assuming it's signed.
# The argument is given as two `int64' pieces in L1 and H1.
# The value is stored as two `int64' pieces in *LV and *HV.

def neg_double(l1, h1, lv, hv):
	if l1 == 0 :
		lv = 0
		hv = -h1
		if (hv & h1) < 0 :
			return 1
		else:
			return 0
		
	else :
		lv = -l1
		hv = ~h1
		return 0	

# Divide doubleword integer LNUM, HNUM by doubleword integer LDEN, HDEN
# for a quotient (stored in *LQUO, *HQUO) and remainder (in *LREM, *HREM).
# CODE is a tree code for a kind of division, one of
# TRUNC_DIV_EXPR, FLOOR_DIV_EXPR, CEIL_DIV_EXPR, ROUND_DIV_EXPR
# or EXACT_DIV_EXPR
# It controls how the quotient is rounded to a integer.
# Return nonzero if the operation overflows.
# UNS nonzero says do unsigned division.

def div_and_round_double(uns, lnum_orig, hnum_orig, lden_orig, hden_orig, lquo, hquo, lrem, hrem) :

    num = [0, 0, 0, 0, 0] # extra element for scaling.
    den = [0, 0, 0, 0]
    quo = [0, 0, 0, 0]
    carry = overflow = quo_neg = 0   #register UNSIGNEDINT64 carry = 0;
    lnum = lnum_orig
    hnum = hnum_orig
    lden = lden_orig
    hden = hden_orig
    
    # calculate quotient sign and convert operands to unsigned.
    if uns != 0 :
        if hnum < 0 :
            quo_neg = ~quo_neg #~ quo_neg;
            # (minimum integer) / (-1) is the only overflow case.
            if (neg_double(lnum, hnum, lnum, hnum) != 0) and ((lden & hden) == -1) :
                overflow = 1
        if hden < 0 :
            quo_neg = ~quo_neg  #~ quo_neg;
            neg_double(lden, hden, lden, hden)
            
    if hnum == 0 and hden == 0 : # single precision
        hquo = hrem = 0
        # This unsigned division rounds toward zero.
        lquo = int((lnum) / (lden))
        # if result is negative, make it so.
        if quo_neg != 0 :
            neg_double(lquo, hquo, lquo, hquo)
		# compute trial remainder:  rem = num - (quo * den)
        mul_double(lquo, hquo, lden_orig, hden_orig, lrem, hrem)
        neg_double(lrem, hrem, lrem, hrem)
        add_double(lnum_orig, hnum_orig, lrem, hrem, lrem, hrem)
        
        return overflow
        
    if hnum == 0 : # trivial case: dividend < divisor 
        hquo = lquo = 0
        hrem = hnum
        lrem = lnum
        # if result is negative, make it so.
        if quo_neg != 0 :
            neg_double(lquo, hquo, lquo, hquo)
		# compute trial remainder:  rem = num - (quo * den)
        mul_double(lquo, hquo, lden_orig, hden_orig, lrem, hrem)
        neg_double(lrem, hrem, lrem, hrem)
        add_double(lnum_orig, hnum_orig, lrem, hrem, lrem, hrem)
        
        return overflow
        
    encodeNum(num[:4], lnum, hnum)
    encodeNum(den[:4], lden, hden)
    
    # Special code for when the divisor < BASE
    if hden == 0 and (lden < base()) :
        # hnum != 0 already checked.
        i = 4 - 1
        while i >= 0 :
            work = (num[i]) + carry*base()
            quo[i] = int((work) / (lden))
            carry = work % (lden)
            i -=1
    else:
		# Full double precision division,
		# with thanks to Don Knuth's "Seminumerical Algorithms".
		# Find the highest non-zero divisor digit.
        i = 4 - 1
        while(1) :
            if den[i] != 0 :
                den_hi_sig = i
                break
            i -=1	

		# Insure that the first digit of the divisor is at least BASE/2.
		# This is required by the quotient digit estimation algorithm. 
        
        scale = int(base() / (den[den_hi_sig]+1))
        if scale > 1 :  # scale divisor and dividend 
            carry = 0
            i = 0
            while i <= 4-1 :
                work = (num[i] * scale) + carry
                num[i] = lowPart(work)
                carry = highPart(work)
                i +=1
                
            num[4] = carry
            carry = 0
            i = 0
            while i <= 4-1 : 
                work = (den[i] * scale) + carry
                den[i] = lowPart(work)
                carry = highPart(work)
                if den[i] != 0 :
                    den_hi_sig = i
                i +=1
                
        num_hi_sig = 4

		# Main loop
        i = num_hi_sig - den_hi_sig - 1
        while i >= 0 : 
			# guess the next quotient digit, quo_est, by dividing the first
			# two remaining dividend digits by the high order quotient digit.
			# quo_est is never low and is at most 2 high.
            num_hi_sig = i + den_hi_sig + 1
            work = num[num_hi_sig]*base() + num[num_hi_sig-1]
            if num[num_hi_sig] != den[den_hi_sig] :
                quo_est = int(work / den[den_hi_sig])
            else :
                quo_est = base() - 1
                
            # refine quo_est so it's usually correct, and at most one high.
            tmp = work - quo_est*den[den_hi_sig]
            if tmp < base() and den[den_hi_sig-1]*quo_est > (tmp*base()+num[num_hi_sig-2]) :
                quo_est -=1
			
			# Try QUO_EST as the quotient digit, by multiplying the
			# divisor by QUO_EST and subtracting from the remaining dividend.
			# Keep in mind that QUO_EST is the I - 1st digit.
            
            carry = 0
            j = 0
            while j <= den_hi_sig :
                work = quo_est*den[j] + carry
                carry = highPart(work)
                work = num[i+j] - lowPart(work)
                num[i+j] = lowPart(work)
                if highPart(work) != 0 :
                    carry = carry + 1
                else :
                    carry = 0
                j +=1    
			
			# if quo_est was high by one, then num[i] went negative and
			# we need to correct things.
            
            if num[num_hi_sig] < carry :
                quo_est -=1
                carry = 0 # add divisor back in 
                j = 0
                while j <= den_hi_sig :
                    work = num[i+j] + den[j] + carry
                    carry = highPart(work)
                    num[i+j] = lowPart(work)
                    j +=1
                num[num_hi_sig] += carry
                
            # store the quotient digit.
            quo[i] = quo_est         
            i -=1
            
    decodeNum(quo[:4], lquo, hquo)

	# if result is negative, make it so.
    if quo_neg != 0 :
        neg_double(*lquo, *hquo, lquo, hquo)
	
    # compute trial remainder:  rem = num - (quo * den)
    mul_double(*lquo, *hquo, lden_orig, hden_orig, lrem, hrem)
    neg_double(*lrem, *hrem, lrem, hrem)
    add_double(lnum_orig, hnum_orig, *lrem, *hrem, lrem, hrem)
    
    return overflow

