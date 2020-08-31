class Solution(object):
    def longestPalindrome(self, s):
        """
        :type s: str
        :rtype: str
        """
        slot = []
        maxslot = []
        if len(s) <= 1:
            return s
        for c in s:
            left = s.index(c)
            r = len(s)
            for right in self.checkright(c, s, r):
                if left != right:
                    slot = s[left:right]
                    slot.reverse()
                if slot == s[left:right]:
                    if len(slot) > len(maxslot):
                        maxslot = s[left:right]
        return maxslot

    def checkright(self, c, s,r):
        try:
            if s.index(c) != r:
                right = s.rindex(c, s.index(c), self.checkright(c, s,r))
                r= right
                print(right)
        except:
            print("String is empty or not valid")
            right = 0
        return right