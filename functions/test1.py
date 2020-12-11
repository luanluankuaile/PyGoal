class Solution(object):
    def twoSum(self,nums,target):
        try:
            for i in range(len(nums)):
                for j in range(i+1,len(nums)):
                    if nums[i]+nums[j]==target:
                       a=nums[i]
                       b=nums[j]
                       index1 = nums.index(a)
                       if a==b:
                           index2 = nums.index(b,index1+1)
                       else:
                           index2=nums.index(b)
                       print(str(a), str(b))
                       print("indexes are " + str(index1) + " and " + str(index2))
                       return index1, index2
                    elif nums[i]+nums[j]<target:
                       continue
        except IndexError:
                    print("no correct answers")

solution=Solution()
print(solution.twoSum([-3,4,3,90],0))




