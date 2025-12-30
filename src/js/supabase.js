// Supabase Configuration
const SUPABASE_URL = 'https://vawrdqreibhlfsfvxbpv.supabase.co';
const SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZhd3JkcXJlaWJobGZzZnZ4YnB2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjcwNzg1MTAsImV4cCI6MjA4MjY1NDUxMH0.-mAobZK_dc3QOwey3Z8NbrtybWPoPRfBqW_IN0gehl8';

const supabase = window.supabase.createClient(SUPABASE_URL, SUPABASE_KEY);

export default supabase;
