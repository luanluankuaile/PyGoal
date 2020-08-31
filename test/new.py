import time
"""一个字符串，寻找字符串最长回文子串并返回"""
def longestPalindrome(s):
    start=time.time()
    maxs = ""
    max_list = []
    if len(s) <= 1:
        maxs = s
    elif len(s) > 1000:
        print("String is too long")
    else:
        ls = list(map(str, list(s)))
        for left in range(0, len(ls) - 1):
            r = len(s)
            while r != left:
                right = s.rindex(ls[left], left, r)
                if left != right:
                    slot = ls[left:(right + 1)]
                    slot.reverse()
                    if ls[left:(right + 1)] == slot:
                        if len(slot) > len(max_list):
                            max_list = slot
                            break
                r = right
        if max_list == []:
            max_list = ls[0]
        maxs = "".join(max_list)
    print(maxs)
    end=time.time()
    print(end-start)
    return maxs

longestPalindrome("")