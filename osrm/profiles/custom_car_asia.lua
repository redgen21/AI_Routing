-- Asia car profile wrapper.
-- Reuse the project car profile while enabling left-hand-driving guidance.

local profile = dofile('/profiles/custom_car.lua')
local base_setup = profile.setup

profile.setup = function()
  local result = base_setup()
  result.properties.left_hand_driving = true
  result.lane_markings_penalty = result.lane_markings_penalty or 0.75
  return result
end

return profile
