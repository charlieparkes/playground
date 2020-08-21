class Solution:
    def twoSum(self, nums: List[int], target: int) -> List[int]:
        n_nums = len(nums)
        for i in range(n_nums):
            for j in range(i, n_nums):
                if i != j and (nums[i] + nums[j]) == target:
                    return [i, j]
        
